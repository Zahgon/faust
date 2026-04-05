"""Tables (changelog stream)."""
import asyncio
import typing

from typing import Any, MutableMapping, Optional, Set, Tuple, cast

from mode import Service
from mode.utils.queues import ThrowableQueue

from faust.types import AppT, ChannelT, StoreT, TP
from faust.types.tables import CollectionT, TableManagerT
from faust.utils.tracing import traced_from_parent_span

from .recovery import Recovery

if typing.TYPE_CHECKING:
    from faust.app import App as _App
else:
    class _App: ...  # noqa

__all__ = [
    'TableManager',
]


class TableManager(Service, TableManagerT):
    """Manage tables used by Faust worker."""

    _channels: MutableMapping[CollectionT, ChannelT]
    _changelogs: MutableMapping[str, CollectionT]
    #: event that when set we cannot add any more tables.
    _tables_finalized: asyncio.Event
    #: event set when all tables have been registered
    _tables_registed: asyncio.Event
    #: event set when table recovery has started.
    _recovery_started: asyncio.Event
    _changelog_queue: Optional[ThrowableQueue]
    _pending_persisted_offsets: MutableMapping[TP, Tuple[StoreT, int]]

    _recovery: Optional[Recovery] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.app = app
        self.data: MutableMapping = {}
        self._changelog_queue = None
        self._channels = {}
        self._changelogs = {}
        self._tables_finalized = asyncio.Event(loop=self.loop)
        self._tables_registered = asyncio.Event(loop=self.loop)
        self._recovery_started = asyncio.Event(loop=self.loop)

        self.actives_ready = False
        self.standbys_ready = False
        self._pending_persisted_offsets = {}

    def persist_offset_on_commit(self,
                                 store: StoreT,
                                 tp: TP,
                                 offset: int) -> None:
        """Mark the persisted offset for a TP to be saved on commit.

        This is used for "exactly_once" processing guarantee.
        Instead of writing the persisted offset to RocksDB when the message
        is sent, we write it to disk when the offset is committed.
        """
        pass

    def on_commit(self, offsets: MutableMapping[TP, int]) -> None:
        """Call when committing source topic partitions."""
        pass

    def on_commit_tp(self, tp: TP) -> None:
        """Call when committing source topic partition used by this table."""
        pass

    def on_rebalance_start(self) -> None:
        """Call when a new rebalancing operation starts."""
        pass

    def on_actives_ready(self) -> None:
        """Call when actives are fully up-to-date."""
        pass

    def on_standbys_ready(self) -> None:
        """Call when standbys are fully up-to-date and ready for failover."""
        pass

    def __hash__(self) -> int:
        return object.__hash__(self)

    @property
    def changelog_topics(self) -> Set[str]:
        """Return the set of known changelog topics."""
        pass

    @property
    def changelog_queue(self) -> ThrowableQueue:
        """Queue used to buffer changelog events."""
        pass

    @property
    def recovery(self) -> Recovery:
        """Recovery service used by this table manager."""
        pass

    def add(self, table: CollectionT) -> CollectionT:
        """Add table to be managed by this table manager."""
        if self._tables_finalized.is_set():
            raise RuntimeError('Too late to add tables at this point')
        assert table.name is not None
        if table.name in self:
            raise ValueError(f'Table with name {table.name!r} already exists')
        self[table.name] = table
        self._changelogs[table.changelog_topic.get_topic_name()] = table
        return table

    async def on_start(self) -> None:
        """Call when table manager is starting."""
        pass

    async def wait_until_tables_registered(self) -> None:
        pass

    async def _update_channels(self) -> None:
        pass

    async def on_stop(self) -> None:
        """Call when table manager is stopping."""
        pass

    def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Call when cluster is rebalancing and partitions revoked."""
        pass

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when the cluster is rebalancing."""
        self._recovery_started.set()  # cannot add more tables.
        T = traced_from_parent_span()
        for table in self.values():
            await T(table.on_rebalance)(assigned, revoked, newly_assigned)

        await asyncio.sleep(0)
        await T(self._update_channels)()
        await asyncio.sleep(0)
        await T(self.recovery.on_rebalance)(assigned, revoked, newly_assigned)

    async def wait_until_recovery_completed(self) -> bool:
        pass
