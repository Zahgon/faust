"""Table recovery after rebalancing."""
import asyncio
import statistics
import typing

from collections import defaultdict, deque
from time import monotonic
from typing import (
    Any,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    NamedTuple,
    Optional,
    Set,
    cast,
)

import opentracing
from mode import Service
from mode.services import WaitArgT
from mode.utils.locks import Event
from mode.utils.times import humanize_seconds, humanize_seconds_ago
from mode.utils.typing import Counter, Deque

from faust.exceptions import ConsistencyError
from faust.types import AppT, EventT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.types.transports import ConsumerT
from faust.utils import terminal
from faust.utils.terminal.tables import TableDataT  # List[List[str]]
from faust.utils.tracing import finish_span, traced_from_parent_span

if typing.TYPE_CHECKING:
    from faust.app import App as _App
    from .manager import TableManager as _TableManager
else:
    class _App: ...           # noqa
    class _TableManager: ...  # noqa

E_PERSISTED_OFFSET = '''\
The persisted offset for changelog topic partition {0} is higher
than the last offset in that topic (highwater) ({1} > {2}).

Most likely you have removed data from the topics without
removing the RocksDB database file for this partition.
'''


class RecoveryStats(NamedTuple):
    highwater: int
    offset: int
    remaining: int


RecoveryStatsMapping = Mapping[TP, RecoveryStats]


class ServiceStopped(Exception):
    """The recovery service was stopped."""


class RebalanceAgain(Exception):
    """During rebalance, another rebalance happened."""


class Recovery(Service):
    """Service responsible for recovering tables from changelog topics."""

    app: AppT

    tables: _TableManager

    stats_interval: float = 5.0

    #: Set of standby topic partitions.
    standby_tps: Set[TP]

    #: Set of active topic partitions.
    active_tps: Set[TP]

    actives_for_table: MutableMapping[CollectionT, Set[TP]]
    standbys_for_table: MutableMapping[CollectionT, Set[TP]]

    #: Mapping from topic partition to table
    tp_to_table: MutableMapping[TP, CollectionT]

    #: Active offset by topic partition.
    active_offsets: Counter[TP]

    #: Standby offset by topic partition.
    standby_offsets: Counter[TP]

    #: Mapping of highwaters by topic partition.
    highwaters: Counter[TP]

    #: Active highwaters by topic partition.
    active_highwaters: Counter[TP]

    #: Standby highwaters by topic partition.
    standby_highwaters: Counter[TP]

    _signal_recovery_start: Optional[Event] = None
    _signal_recovery_end: Optional[Event] = None
    _signal_recovery_reset: Optional[Event] = None

    completed: Event
    in_recovery: bool = False
    standbys_pending: bool = False
    recovery_delay: float

    #: Changelog event buffers by table.
    #: These are filled by background task `_slurp_changelog`,
    #: and need to be flushed before starting new recovery/stopping.
    buffers: MutableMapping[CollectionT, List[EventT]]

    #: Cache of max buffer size by topic partition..
    buffer_sizes: MutableMapping[TP, int]

    #: Time in seconds after we warn that no flush has happened.
    flush_timeout_secs: float = 120.0

    #: Time in seconds after we warn that no events have been received.
    event_timeout_secs: float = 30.0

    #: Time of last event received by active TP
    _active_events_received_at: MutableMapping[TP, float]

    #: Time of last event received by standby TP
    _standby_events_received_at: MutableMapping[TP, float]

    #: Time of last event received (for any active TP)
    _last_active_event_processed_at: Optional[float]

    #: Time of last buffer flush
    _last_flush_at: Optional[float] = None

    #: Time when recovery last started
    _recovery_started_at: Optional[float] = None

    #: Time when recovery last ended
    _recovery_ended_at: Optional[float] = None

    _recovery_span: Optional[opentracing.Span] = None
    _actives_span: Optional[opentracing.Span] = None
    _standbys_span: Optional[opentracing.Span] = None

    #: List of last 100 processing timestamps (monotonic).
    #: Updated after processing every changelog record,
    #: used to estimate time remaining.
    _processing_times: Deque[float]

    #: Number of entries in _processing_times before
    #: we can give an estimate of time remaining.
    num_samples_required_for_estimate = 1000

    def __init__(self,
                 app: AppT,
                 tables: TableManagerT,
                 **kwargs: Any) -> None:
        self.app = app
        self.tables = cast(_TableManager, tables)

        self.standby_tps = set()
        self.active_tps = set()

        self.tp_to_table = {}
        self.active_offsets = Counter()
        self.standby_offsets = Counter()

        self.active_highwaters = Counter()
        self.standby_highwaters = Counter()
        self.completed = Event()

        self.buffers = defaultdict(list)
        self.buffer_sizes = {}
        self.recovery_delay = self.app.conf.stream_recovery_delay

        self.actives_for_table = defaultdict(set)
        self.standbys_for_table = defaultdict(set)

        self._active_events_received_at = {}
        self._standby_events_received_at = {}
        self._processing_times = deque()

        super().__init__(**kwargs)

    @property
    def signal_recovery_start(self) -> Event:
        """Event used to signal that recovery has started."""
        pass

    @property
    def signal_recovery_end(self) -> Event:
        """Event used to signal that recovery has ended."""
        pass

    @property
    def signal_recovery_reset(self) -> Event:
        """Event used to signal that recovery is restarting."""
        pass

    async def on_stop(self) -> None:
        """Call when recovery service stops."""
        pass

    def add_active(self, table: CollectionT, tp: TP) -> None:
        """Add changelog partition to be used for active recovery."""
        self.active_tps.add(tp)
        self.actives_for_table[table].add(tp)
        self._add(table, tp, self.active_offsets)

    def add_standby(self, table: CollectionT, tp: TP) -> None:
        """Add changelog partition to be used for standby recovery."""
        self.standby_tps.add(tp)
        self.standbys_for_table[table].add(tp)
        self._add(table, tp, self.standby_offsets)

    def _add(self, table: CollectionT, tp: TP, offsets: Counter[TP]) -> None:
        self.tp_to_table[tp] = table
        persisted_offset = table.persisted_offset(tp)
        if persisted_offset is not None:
            offsets[tp] = persisted_offset
        offsets.setdefault(tp, None)  # type: ignore

    def revoke(self, tp: TP) -> None:
        """Revoke assignment of table changelog partition."""
        self.standby_offsets.pop(tp, None)
        self.standby_highwaters.pop(tp, None)
        self.active_offsets.pop(tp, None)
        self.active_highwaters.pop(tp, None)

    def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Call when rebalancing and partitions are revoked."""
        pass

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when cluster is rebalancing."""
        app = self.app
        assigned_standbys = app.assignor.assigned_standbys()
        assigned_actives = app.assignor.assigned_actives()

        for tp in revoked:
            await asyncio.sleep(0)
            self.revoke(tp)

        self.standby_tps.clear()
        self.active_tps.clear()
        self.actives_for_table.clear()
        self.standbys_for_table.clear()

        for tp in assigned_standbys:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_standby(table, tp)
            await asyncio.sleep(0)
        await asyncio.sleep(0)
        for tp in assigned_actives:
            table = self.tables._changelogs.get(tp.topic)
            if table is not None:
                self.add_active(table, tp)
            await asyncio.sleep(0)
        await asyncio.sleep(0)

        active_offsets = {
            tp: offset
            for tp, offset in self.active_offsets.items()
            if tp in self.active_tps
        }
        self.active_offsets.clear()
        self.active_offsets.update(active_offsets)

        await asyncio.sleep(0)

        rebalancing_span = cast(_App, self.app)._rebalancing_span
        if app.tracer and rebalancing_span:
            self._recovery_span = app.tracer.get_tracer('_faust').start_span(
                'recovery',
                child_of=rebalancing_span,
            )
            app._span_add_default_tags(self._recovery_span)
        self.signal_recovery_reset.clear()
        self.signal_recovery_start.set()

    async def _resume_streams(self) -> None:
        pass

    @Service.task
    async def _restart_recovery(self) -> None:
        pass

    def _set_recovery_started(self) -> None:
        pass

    def _set_recovery_ended(self) -> None:
        pass

    def active_remaining_seconds(self, remaining: float) -> str:
        pass

    def _estimated_active_remaining_secs(
            self, remaining: float) -> Optional[float]:
        pass

    async def _wait(self, coro: WaitArgT) -> None:
        pass

    async def on_recovery_completed(self) -> None:
        """Call when active table recovery is completed."""
        pass

    async def _build_highwaters(self,
                                consumer: ConsumerT,
                                tps: Set[TP],
                                destination: Counter[TP],
                                title: str) -> None:
        # -- Build highwater
        pass

    def _highwater_logtable(self, highwaters: Mapping[TP, int], *,
                            title: str) -> str:
        pass

    def _consolidate_table_keys(self, data: TableDataT) -> Iterator[List[str]]:
        """Format terminal log table to reduce noise from duplicate keys.

        We log tables where the first row is the name of the topic,
        and it gets noisy when that name is repeated over and over.

        This function replaces repeating topic names
        with the ditto mark.

        Note:
            Data must be sorted.
        """
        pass

    async def _build_offsets(self,
                             consumer: ConsumerT,
                             tps: Set[TP],
                             destination: Counter[TP],
                             title: str) -> None:
        # -- Update offsets
        # Offsets may have been compacted, need to get to the recent ones
        pass

    def _start_offsets_logtable(self, offsets: Mapping[TP, int], *,
                                title: str) -> str:
        pass

    async def _seek_offsets(self,
                            consumer: ConsumerT,
                            tps: Set[TP],
                            offsets: Counter[TP],
                            title: str) -> None:
        # Seek to new offsets
        pass

    @Service.task
    async def _slurp_changelogs(self) -> None:
        pass

    def flush_buffers(self) -> None:
        """Flush changelog buffers."""
        pass

    def need_recovery(self) -> bool:
        """Return :const:`True` if recovery is required."""
        pass

    def active_remaining(self) -> Counter[TP]:
        """Return counter of remaining changes by active partition."""
        pass

    def standby_remaining(self) -> Counter[TP]:
        """Return counter of remaining changes by standby partition."""
        pass

    def active_remaining_total(self) -> int:
        """Return number of changes remaining for actives to be up-to-date."""
        pass

    def standby_remaining_total(self) -> int:
        """Return number of changes remaining for standbys to be up-to-date."""
        pass

    def active_stats(self) -> RecoveryStatsMapping:
        """Return current active recovery statistics."""
        pass

    def standby_stats(self) -> RecoveryStatsMapping:
        """Return current standby recovery statistics."""
        pass

    def _stats_to_logtable(
            self,
            title: str,
            stats: RecoveryStatsMapping) -> str:
        pass

    @Service.task
    async def _publish_stats(self) -> None:
        """Emit stats (remaining to fetch) while in active recovery."""
        pass

    async def _verify_remaining(
            self,
            now: float,
            stats: RecoveryStatsMapping) -> None:
        pass

    def _current_total_buffer_size(self) -> int:
        pass

    def _is_changelog_tp(self, tp: TP) -> bool:
        pass
