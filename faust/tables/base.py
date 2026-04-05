"""Base class Collection for Table and future data structures."""
import abc
import time

from contextlib import suppress
from collections import defaultdict
from functools import lru_cache
from datetime import datetime
from heapq import heappop, heappush
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
    no_type_check,
)

from mode.utils.futures import maybe_async
from mode import Seconds, Service
from yarl import URL

from faust import stores
from faust import joins
from faust.exceptions import PartitionsMismatch
from faust.streams import current_event
from faust.types import (
    AppT,
    CodecArg,
    EventT,
    FieldDescriptorT,
    FutureMessage,
    JoinT,
    RecordMetadata,
    SchemaT,
    TP,
    TopicT,
)
from faust.types.models import ModelArg, ModelT
from faust.types.stores import StoreT
from faust.types.streams import JoinableT, StreamT
from faust.types.tables import (
    ChangelogEventCallback,
    CollectionT,
    RecoverCallback,
    RelativeHandler,
    WindowCloseCallback,
)
from faust.types.windows import WindowRange, WindowT

__all__ = ['Collection']

TABLE_CLEANING = 'CLEANING'

E_SOURCE_PARTITIONS_MISMATCH = '''\
The source topic {source_topic!r} for table {table_name!r}
has {source_n} partitions, but the changelog
topic {change_topic!r} has {change_n} partitions.

Please make sure the topics have the same number of partitions
by configuring Kafka correctly.
'''


class Collection(Service, CollectionT):
    """Base class for changelog-backed data structures stored in Kafka."""

    _store: Optional[URL]
    _changelog_topic: Optional[TopicT]
    _partition_timestamp_keys: MutableMapping[
        Tuple[int, float], MutableSet[Tuple[Any, WindowRange]]]
    _partition_timestamps: MutableMapping[int, List[float]]
    _partition_latest_timestamp: MutableMapping[int, float]
    _recover_callbacks: MutableSet[RecoverCallback]
    _data: Optional[StoreT] = None
    _changelog_compacting: Optional[bool] = True
    _changelog_deleting: Optional[bool] = None

    @abc.abstractmethod
    def _has_key(self, key: Any) -> bool:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _get_key(self, key: Any) -> Any:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _set_key(self, key: Any, value: Any) -> None:  # pragma: no cover
        ...

    @abc.abstractmethod
    def _del_key(self, key: Any) -> None:  # pragma: no cover
        ...

    def __init__(self,
                 app: AppT,
                 *,
                 name: str = None,
                 default: Callable[[], Any] = None,
                 store: Union[str, URL] = None,
                 schema: SchemaT = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 partitions: int = None,
                 window: WindowT = None,
                 changelog_topic: TopicT = None,
                 help: str = None,
                 on_recover: RecoverCallback = None,
                 on_changelog_event: ChangelogEventCallback = None,
                 recovery_buffer_size: int = 1000,
                 standby_buffer_size: int = None,
                 extra_topic_configs: Mapping[str, Any] = None,
                 recover_callbacks: Set[RecoverCallback] = None,
                 options: Mapping[str, Any] = None,
                 use_partitioner: bool = False,
                 on_window_close: WindowCloseCallback = None,
                 is_global: bool = False,
                 **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app
        self.name = cast(str, name)  # set lazily so CAN BE NONE!
        self.default = default
        self._store = URL(store) if store else None
        self.schema = schema
        self.key_type = key_type
        self.value_type = value_type
        self.partitions = partitions
        self.window = window
        self._changelog_topic = changelog_topic
        self.extra_topic_configs = extra_topic_configs or {}
        self.help = help or ''
        self._on_changelog_event = on_changelog_event
        self.recovery_buffer_size = recovery_buffer_size
        self.standby_buffer_size = standby_buffer_size or recovery_buffer_size
        self.use_partitioner = use_partitioner
        self._on_window_close = on_window_close
        self.last_closed_window = 0.0
        self.is_global = is_global
        assert self.recovery_buffer_size > 0 and self.standby_buffer_size > 0

        self.options = options

        # Setting Serializers from key_type and value_type
        # Possible values json and raw
        # Fallback to json
        self.key_serializer = self._serializer_from_type(self.key_type)
        self.value_serializer = self._serializer_from_type(self.value_type)

        # Table key expiration
        self._partition_timestamp_keys = defaultdict(set)
        self._partition_timestamps = defaultdict(list)
        self._partition_latest_timestamp = defaultdict(int)

        self._recover_callbacks = set(recover_callbacks or [])
        if on_recover:
            self.on_recover(on_recover)

        # Aliases
        self._sensor_on_get = self.app.sensors.on_table_get
        self._sensor_on_set = self.app.sensors.on_table_set
        self._sensor_on_del = self.app.sensors.on_table_del

    def _serializer_from_type(
            self, typ: Optional[ModelArg]) -> Optional[CodecArg]:
        if typ is bytes:
            return 'raw'
        serializer = None
        with suppress(AttributeError):
            serializer = typ._options.serializer  # type: ignore
        return serializer or 'json'

    def __hash__(self) -> int:
        # We have to override MutableMapping __hash__, so that this table
        # can be registered in the app.tables mapping.
        return object.__hash__(self)

    def _new_store(self) -> StoreT:
        pass

    def _new_store_by_url(self, url: Union[str, URL]) -> StoreT:
        pass

    @property  # type: ignore
    @no_type_check  # XXX https://github.com/python/mypy/issues/4125
    def data(self) -> StoreT:
        """Underlying table storage."""
        pass

    async def on_start(self) -> None:
        """Call when table starts."""
        pass

    def on_recover(self, fun: RecoverCallback) -> RecoverCallback:
        """Add function as callback to be called on table recovery."""
        assert fun not in self._recover_callbacks
        self._recover_callbacks.add(fun)
        return fun

    def info(self) -> Mapping[str, Any]:
        """Return table attributes as dictionary."""
        # Used to recreate object in .clone()
        return {
            'app': self.app,
            'name': self.name,
            'default': self.default,
            'store': self._store,
            'schema': self.schema,
            'key_type': self.key_type,
            'value_type': self.value_type,
            'partitions': self.partitions,
            'window': self.window,
            'changelog_topic': self._changelog_topic,
            'recover_callbacks': self._recover_callbacks,
            'on_changelog_event': self._on_changelog_event,
            'recovery_buffer_size': self.recovery_buffer_size,
            'standby_buffer_size': self.standby_buffer_size,
            'extra_topic_configs': self.extra_topic_configs,
            'use_partitioner': self.use_partitioner,
        }

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the last persisted offset for topic partition."""
        return self.data.persisted_offset(tp)

    async def need_active_standby_for(self, tp: TP) -> bool:
        """Return :const:`False` if we have access to partition data."""
        pass

    def reset_state(self) -> None:
        """Reset local state."""
        pass

    def send_changelog(self,
                       partition: Optional[int],
                       key: Any,
                       value: Any,
                       key_serializer: CodecArg = None,
                       value_serializer: CodecArg = None) -> FutureMessage:
        """Send modification event to changelog topic."""
        pass

    def _send_changelog(self,
                        event: Optional[EventT],
                        key: Any,
                        value: Any,
                        key_serializer: CodecArg = None,
                        value_serializer: CodecArg = None) -> None:
        # XXX compat version of send_changelog that needs event argument.
        pass

    def partition_for_key(self, key: Any) -> Optional[int]:
        """Return partition number for table key.

        Always returns :const:`None` when :attr:`use_partitioner`
        is enabled.

        Returns:
            Optional[int]: specific partition or :const:`None` if
                the producer should select partition using its partitioner.
        """
        pass

    @lru_cache()
    def _verify_source_topic_partitions(self, source_topic: str) -> None:
        pass

    def _on_changelog_sent(self, fut: FutureMessage) -> None:
        # This is what keeps the offset in RocksDB so that at startup
        # we know what offsets we already have data for in the database.
        #
        # Kafka Streams has a global ".checkpoint" file, but we store
        # it in each individual RocksDB database file.
        # Every partition in the table will have its own database file,
        #  this is required as partitions can easily move from and to
        #  machine as nodes die and recover.
        pass

    @Service.task
    @Service.transitions_to(TABLE_CLEANING)
    async def _clean_data(self) -> None:
        pass

    async def _del_old_keys(self) -> None:
        pass

    async def on_window_close(self, key: Any, value: Any) -> None:
        pass

    def _should_expire_keys(self) -> bool:
        pass

    def _maybe_set_key_ttl(self, key: Any, partition: int) -> None:
        pass

    def _maybe_del_key_ttl(self, key: Any, partition: int) -> None:
        pass

    def _changelog_topic_name(self) -> str:
        pass

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        """Right join of this table and another stream/table."""
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Left join of this table and another stream/table."""
        pass

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Inner join of this table and another stream/table."""
        pass

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Outer join of this table and another stream/table."""
        pass

    def _join(self, join_strategy: JoinT) -> StreamT:
        # TODO
        raise NotImplementedError('TODO')

    def clone(self, **kwargs: Any) -> Any:
        """Clone table instance."""
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        """Combine tables and streams."""
        # TODO
        raise NotImplementedError('TODO')

    def contribute_to_stream(self, active: StreamT) -> None:
        """Contribute table to stream join."""
        # TODO  See Stream.contribute_to_stream()
        # Should probably connect to Table changelog.
        ...

    async def remove_from_stream(self, stream: StreamT) -> None:
        """Remove table from stream join after stream stopped."""
        # TODO See Stream.remove_from_stream()
        # Should stop any services started to support joining this table
        # with one or more streams.
        ...

    def _new_changelog_topic(self,
                             *,
                             retention: Seconds = None,
                             compacting: bool = None,
                             deleting: bool = None) -> TopicT:
        pass

    def __copy__(self) -> Any:
        return self.clone()

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def _apply_window_op(self, op: Callable[[Any, Any], Any], key: Any,
                         value: Any, timestamp: float) -> None:
        get_ = self._get_key
        set_ = self._set_key
        for window_range in self._window_ranges(timestamp):
            set_((key, window_range), op(get_((key, window_range)), value))

    def _set_windowed(self, key: Any, value: Any, timestamp: float) -> None:
        pass

    def _del_windowed(self, key: Any, timestamp: float) -> None:
        pass

    def _window_ranges(self, timestamp: float) -> Iterator[WindowRange]:
        window = cast(WindowT, self.window)
        for window_range in window.ranges(timestamp):
            yield window_range

    def _relative_now(self, event: EventT = None) -> float:
        # get current timestamp
        event = event if event is not None else current_event()
        if event is None:
            return time.time()
        return self._partition_latest_timestamp[event.message.partition]

    def _relative_event(self, event: EventT = None) -> float:
        event = event if event is not None else current_event()
        # get event timestamp
        if event is None:
            raise RuntimeError('Operation outside of stream iteration')
        return event.message.timestamp

    def _relative_field(self, field: FieldDescriptorT) -> RelativeHandler:
        def to_value(event: EventT = None) -> Union[float, datetime]:
            pass

        return to_value

    def _relative_timestamp(self, timestamp: float) -> RelativeHandler:
        def handler(event: EventT = None) -> Union[float, datetime]:
            return timestamp

        return handler

    def _windowed_now(self, key: Any) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.earliest(self._relative_now())))

    def _windowed_timestamp(self, key: Any, timestamp: float) -> Any:
        window = cast(WindowT, self.window)
        return self._get_key((key, window.current(timestamp)))

    def _windowed_contains(self, key: Any, timestamp: float) -> bool:
        pass

    def _windowed_delta(self, key: Any, d: Seconds,
                        event: EventT = None) -> Any:
        pass

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when cluster is rebalancing."""
        await self.data.on_rebalance(self, assigned, revoked, newly_assigned)

    async def on_recovery_completed(self,
                                    active_tps: Set[TP],
                                    standby_tps: Set[TP]) -> None:
        """Call when recovery has completed after rebalancing."""
        pass

    async def call_recover_callbacks(self) -> None:
        """Call any configured recovery callbacks after rebalancing."""
        pass

    async def on_changelog_event(self, event: EventT) -> None:
        """Call when a new changelog event is received."""
        pass

    @property
    def label(self) -> str:
        """Return human-readable label used to represent this table."""
        return f'{self.shortlabel}@{self._store}'

    @property
    def shortlabel(self) -> str:
        """Return short label used to represent this table in logs."""
        return f'{type(self).__name__}: {self.name}'

    @property
    def changelog_topic(self) -> TopicT:
        """Return the changelog topic used by this table."""
        pass

    @changelog_topic.setter
    def changelog_topic(self, topic: TopicT) -> None:
        pass

    @property
    def changelog_topic_name(self) -> str:
        pass

    def apply_changelog_batch(self, batch: Iterable[EventT]) -> None:
        """Apply batch of events from changelog topic local table storage."""
        pass

    def _to_key(self, k: Any) -> Any:
        pass

    def _to_value(self, v: Any) -> Any:
        pass

    def _human_channel(self) -> str:
        return f'{type(self).__name__}: {self.name}'

    def _repr_info(self) -> str:
        pass
