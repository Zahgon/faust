"""Consumer - fetching messages and managing consumer state.

The Consumer is responsible for:

   - Holds reference to the transport that created it

   - ... and the app via ``self.transport.app``.

   - Has a callback that usually points back to ``Conductor.on_message``.

   - Receives messages and calls the callback for every message received.

   - Keeps track of the message and its acked/unacked status.

   - The Conductor forwards the message to all Streams that subscribes
     to the topic the message was sent to.

       + Messages are reference counted, and the Conductor increases
         the reference count to the number of subscribed streams.

       + ``Stream.__aiter__`` is set up in a way such that when what is
         iterating over the stream is finished with the message, a
         finally: block will decrease the reference count by one.

       + When the reference count for a message hits zero, the stream will
         call ``Consumer.ack(message)``, which will mark that topic +
         partition + offset combination as "committable"

       + If all the streams share the same key_type/value_type,
         the conductor will only deserialize the payload once.

   - Commits the offset at an interval

      + The Consumer has a background thread that periodically commits the
        offset.

      - If the consumer marked an offset as committable this thread
        will advance the committed offset.

      + To find the offset that it can safely advance to the commit thread
        will traverse the _acked mapping of TP to list of acked offsets, by
        finding a range of consecutive acked offsets (see note in
        _new_offset).

"""
import abc
import asyncio
import gc
import typing

from collections import defaultdict
from time import monotonic
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    ClassVar,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableMapping,
    MutableSet,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
)
from weakref import WeakSet

from mode import Service, ServiceT, flight_recorder, get_logger
from mode.threads import MethodQueue, QueueServiceThread
from mode.utils.futures import notify
from mode.utils.locks import Event
from mode.utils.text import pluralize
from mode.utils.times import Seconds
from faust.exceptions import ProducerSendError
from faust.types import AppT, ConsumerMessage, Message, RecordMetadata, TP
from faust.types.core import HeadersArg
from faust.types.transports import (
    ConsumerCallback,
    ConsumerT,
    PartitionsAssignedCallback,
    PartitionsRevokedCallback,
    ProducerT,
    TPorTopicSet,
    TransactionManagerT,
    TransportT,
)
from faust.types.tuples import FutureMessage
from faust.utils import terminal
from faust.utils.functional import consecutive_numbers
from faust.utils.tracing import traced_from_parent_span

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.app import App as _App
else:
    class _App: ...  # noqa: E701

__all__ = ['Consumer', 'Fetcher']

# These flags are used for Service.diag, tracking what the consumer
# service is currently doing.
CONSUMER_FETCHING = 'FETCHING'
CONSUMER_PARTITIONS_REVOKED = 'PARTITIONS_REVOKED'
CONSUMER_PARTITIONS_ASSIGNED = 'PARTITIONS_ASSIGNED'
CONSUMER_COMMITTING = 'COMMITTING'
CONSUMER_SEEKING = 'SEEKING'
CONSUMER_WAIT_EMPTY = 'WAIT_EMPTY'

logger = get_logger(__name__)

RecordMap = Mapping[TP, List[Any]]


class TopicPartitionGroup(NamedTuple):
    """Tuple of ``(topic, partition, group)``."""

    topic: str
    partition: int
    group: int


def ensure_TP(tp: Any) -> TP:
    """Convert aiokafka ``TopicPartition`` to Faust ``TP``."""
    return tp if isinstance(tp, TP) else TP(tp.topic, tp.partition)


def ensure_TPset(tps: Iterable[Any]) -> Set[TP]:
    """Convert set of aiokafka ``TopicPartition`` to Faust ``TP``."""
    return {ensure_TP(tp) for tp in tps}


class Fetcher(Service):
    """Service fetching messages from Kafka."""

    app: AppT

    logger = logger
    _drainer: Optional[asyncio.Future] = None

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        super().__init__(**kwargs)

    async def on_stop(self) -> None:
        """Call when the fetcher is stopping."""
        pass

    @Service.task
    async def _fetcher(self) -> None:
        pass


class TransactionManager(Service, TransactionManagerT):
    """Manage producer transactions."""

    app: AppT

    transactional_id_format = '{group_id}-{tpg.group}-{tpg.partition}'

    def __init__(self, transport: TransportT,
                 *,
                 consumer: 'ConsumerT',
                 producer: 'ProducerT',
                 **kwargs: Any) -> None:
        self.transport = transport
        self.app = self.transport.app
        self.consumer = consumer
        self.producer = producer
        super().__init__(**kwargs)

    async def flush(self) -> None:
        """Wait for producer to transmit all pending messages."""
        await self.producer.flush()

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Call when the cluster is rebalancing and partitions are revoked."""
        pass

    async def on_rebalance(self,
                           assigned: Set[TP],
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when the cluster is rebalancing."""
        T = traced_from_parent_span()
        # Stop producers for revoked partitions.
        revoked_tids = sorted(self._tps_to_transactional_ids(revoked))
        if revoked_tids:
            self.log.info(
                'Stopping %r transactional %s for %r revoked %s...',
                len(revoked_tids),
                pluralize(len(revoked_tids), 'producer'),
                len(revoked),
                pluralize(len(revoked), 'partition'))
            await T(self._stop_transactions, tids=revoked_tids)(revoked_tids)

        # Start produers for assigned partitions
        assigned_tids = sorted(self._tps_to_transactional_ids(assigned))
        if assigned_tids:
            self.log.info(
                'Starting %r transactional %s for %r assigned %s...',
                len(assigned_tids),
                pluralize(len(assigned_tids), 'producer'),
                len(assigned),
                pluralize(len(assigned), 'partition'))
            await T(self._start_transactions,
                    tids=assigned_tids)(assigned_tids)

    async def _stop_transactions(self, tids: Iterable[str]) -> None:
        pass

    async def _start_transactions(self, tids: Iterable[str]) -> None:
        pass

    def _tps_to_transactional_ids(self, tps: Set[TP]) -> Set[str]:
        return {
            self.transactional_id_format.format(
                tpg=tpg,
                group_id=self.app.conf.id,
            )
            for tpg in self._tps_to_active_tpgs(tps)
        }

    def _tps_to_active_tpgs(self, tps: Set[TP]) -> Set[TopicPartitionGroup]:
        assignor = self.app.assignor
        return {
            TopicPartitionGroup(
                tp.topic,
                tp.partition,
                assignor.group_for_topic(tp.topic),
            )
            for tp in tps
            if not assignor.is_standby(tp)
        }

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        """Schedule message to be sent by producer."""
        group = transactional_id = None
        p = self.consumer.key_partition(topic, key, partition)
        if p is not None:
            group = self.app.assignor.group_for_topic(topic)
            transactional_id = f'{self.app.conf.id}-{group}-{p}'
        return await self.producer.send(
            topic, key, value, p, timestamp, headers,
            transactional_id=transactional_id,
        )

    def send_soon(self, fut: FutureMessage) -> None:
        raise NotImplementedError()

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        fut = await self.send(topic, key, value, partition, timestamp, headers)
        return await fut

    async def commit(self, offsets: Mapping[TP, int],
                     start_new_transaction: bool = True) -> bool:
        """Commit offsets for partitions."""
        pass

    def key_partition(self, topic: str, key: bytes) -> TP:
        raise NotImplementedError()

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        return await self.producer.create_topic(
            topic, partitions, replication,
            config=config,
            timeout=timeout,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def supports_headers(self) -> bool:
        """Return :const:`True` if the Kafka server supports headers."""
        pass


class Consumer(Service, ConsumerT):
    """Base Consumer."""

    app: AppT

    logger = logger

    #: Tuple of exception types that may be raised when the
    #: underlying consumer driver is stopped.
    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = ()

    # Mapping of TP to list of gap in offsets.
    _gap: MutableMapping[TP, List[int]]

    # Mapping of TP to list of acked offsets.
    _acked: MutableMapping[TP, List[int]]

    #: Fast lookup to see if tp+offset was acked.
    _acked_index: MutableMapping[TP, Set[int]]

    #: Keeps track of the currently read offset in each TP
    _read_offset: MutableMapping[TP, Optional[int]]

    #: Keeps track of the currently committed offset in each TP.
    _committed_offset: MutableMapping[TP, Optional[int]]

    #: The consumer.wait_empty() method will set this to be notified
    #: when something acks a message.
    _waiting_for_ack: Optional[asyncio.Future] = None

    #: Used by .commit to ensure only one thread is comitting at a time.
    #: Other thread starting to commit while a commit is already active,
    #: will wait for the original request to finish, and do nothing.
    _commit_fut: Optional[asyncio.Future] = None

    #: Set of unacked messages: that is messages that we started processing
    #: and that we MUST attempt to complete processing of, before
    #: shutting down or resuming a rebalance.
    _unacked_messages: MutableSet[Message]

    #: Time of when the consumer was started.
    _time_start: float

    # How often to poll and track log end offsets.
    _end_offset_monitor_interval: float

    _commit_every: Optional[int]
    _n_acked: int = 0

    _active_partitions: Optional[Set[TP]]
    _paused_partitions: Set[TP]
    _buffered_partitions: Set[TP]

    flow_active: bool = True
    can_resume_flow: Event

    def __init__(self,
                 transport: TransportT,
                 callback: ConsumerCallback,
                 on_partitions_revoked: PartitionsRevokedCallback,
                 on_partitions_assigned: PartitionsAssignedCallback,
                 *,
                 commit_interval: float = None,
                 commit_livelock_soft_timeout: float = None,
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs: Any) -> None:
        assert callback is not None
        self.transport = transport
        self.app = self.transport.app
        self.in_transaction = self.app.in_transaction
        self.callback = callback
        self._on_message_in = self.app.sensors.on_message_in
        self._on_partitions_revoked = on_partitions_revoked
        self._on_partitions_assigned = on_partitions_assigned
        self._commit_every = self.app.conf.broker_commit_every
        self.scheduler = self.app.conf.ConsumerScheduler()  # type: ignore
        self.commit_interval = (
            commit_interval or self.app.conf.broker_commit_interval)
        self.commit_livelock_soft_timeout = (
            commit_livelock_soft_timeout or
            self.app.conf.broker_commit_livelock_soft_timeout)
        self._gap = defaultdict(list)
        self._acked = defaultdict(list)
        self._acked_index = defaultdict(set)
        self._read_offset = defaultdict(lambda: None)
        self._committed_offset = defaultdict(lambda: None)
        self._unacked_messages = WeakSet()
        self._buffered_partitions = set()
        self._waiting_for_ack = None
        self._time_start = monotonic()
        self._end_offset_monitor_interval = self.commit_interval * 2
        self.randomly_assigned_topics = set()
        self.can_resume_flow = Event()
        self._reset_state()
        super().__init__(loop=loop or self.transport.loop, **kwargs)
        self.transactions = self.transport.create_transaction_manager(
            consumer=self,
            producer=self.app.producer,
            beacon=self.beacon,
            loop=self.loop,
        )

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        """Return list of services this consumer depends on."""
        pass

    def _reset_state(self) -> None:
        self._active_partitions = None
        self._paused_partitions = set()
        self._buffered_partitions = set()
        self.can_resume_flow.clear()
        self.flow_active = True
        self._time_start = monotonic()

    async def on_restart(self) -> None:
        """Call when the consumer is restarted."""
        pass

    def _get_active_partitions(self) -> Set[TP]:
        pass

    def _set_active_tps(self, tps: Set[TP]) -> Set[TP]:
        xtps = self._active_partitions = ensure_TPset(tps)  # copy
        xtps.difference_update(self._paused_partitions)
        return xtps

    def on_buffer_full(self, tp: TP) -> None:
        pass

    def on_buffer_drop(self, tp: TP) -> None:
        pass

    @abc.abstractmethod
    async def _commit(
            self,
            offsets: Mapping[TP, int]) -> bool:  # pragma: no cover
        ...

    async def perform_seek(self) -> None:
        """Seek all partitions to their current committed position."""
        pass

    @abc.abstractmethod
    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek all partitions to their committed offsets."""
        ...

    async def seek(self, partition: TP, offset: int) -> None:
        """Seek partition to specific offset."""
        pass

    @abc.abstractmethod
    async def _seek(self, partition: TP, offset: int) -> None:
        ...

    def stop_flow(self) -> None:
        """Block consumer from processing any more messages."""
        self.flow_active = False
        self.can_resume_flow.clear()

    def resume_flow(self) -> None:
        """Allow consumer to process messages."""
        self.flow_active = True
        self.can_resume_flow.set()

    def pause_partitions(self, tps: Iterable[TP]) -> None:
        """Pause fetching from partitions."""
        pass

    def resume_partitions(self, tps: Iterable[TP]) -> None:
        """Resume fetching from partitions."""
        pass

    @abc.abstractmethod
    def _new_topicpartition(
            self, topic: str, partition: int) -> TP:  # pragma: no cover
        ...

    def _is_changelog_tp(self, tp: TP) -> bool:
        pass

    @Service.transitions_to(CONSUMER_PARTITIONS_REVOKED)
    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        """Call during rebalancing when partitions are being revoked."""
        pass

    @Service.transitions_to(CONSUMER_PARTITIONS_ASSIGNED)
    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        """Call during rebalancing when partitions are being assigned."""
        span = self.app._start_span_from_rebalancing('on_partitions_assigned')
        T = traced_from_parent_span(span)
        with span:
            # remove recently revoked tps from set of paused tps.
            self._paused_partitions.intersection_update(assigned)
            # cache set of assigned partitions
            self._set_active_tps(assigned)
            # start callback chain of assigned callbacks.
            #   need to copy set at this point, since we cannot have
            #   the callbacks mutate our active list.
            await T(self._on_partitions_assigned, partitions=assigned)(
                assigned)
        self.app.on_rebalance_return()

    @abc.abstractmethod
    async def _getmany(self,
                       active_partitions: Optional[Set[TP]],
                       timeout: float) -> RecordMap:
        ...

    async def getmany(self,
                      timeout: float) -> AsyncIterator[Tuple[TP, Message]]:
        """Fetch batch of messages from server."""
        pass

    async def _wait_next_records(
            self, timeout: float) -> Tuple[Optional[RecordMap],
                                           Optional[Set[TP]]]:
        pass

    @abc.abstractmethod
    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        ...

    def track_message(self, message: Message) -> None:
        """Track message and mark it as pending ack."""
        pass

    def ack(self, message: Message) -> bool:
        """Mark message as being acknowledged by stream."""
        if not message.acked:
            message.acked = True
            tp = message.tp
            offset = message.offset
            if self.app.topics.acks_enabled_for(message.topic):
                committed = self._committed_offset[tp]
                try:
                    if committed is None or offset > committed:
                        acked_index = self._acked_index[tp]
                        if offset not in acked_index:
                            self._unacked_messages.discard(message)
                            acked_index.add(offset)
                            acked_for_tp = self._acked[tp]
                            acked_for_tp.append(offset)
                            self._n_acked += 1
                            return True
                finally:
                    notify(self._waiting_for_ack)
        return False

    async def _wait_for_ack(self, timeout: float) -> None:
        # arm future so that `ack()` can wake us up
        pass

    @Service.transitions_to(CONSUMER_WAIT_EMPTY)
    async def wait_empty(self) -> None:
        """Wait for all messages that started processing to be acked."""
        pass

    def _clean_unacked_messages(self) -> None:
        # remove actually acked messages from weakset.
        pass

    async def commit_and_end_transactions(self) -> None:
        """Commit all safe offsets and end transaction."""
        pass

    async def on_stop(self) -> None:
        """Call when consumer is stopping."""
        pass

    @Service.task
    async def _commit_handler(self) -> None:
        pass

    @Service.task
    async def _commit_livelock_detector(self) -> None:  # pragma: no cover
        pass

    async def verify_all_partitions_active(self) -> None:
        pass

    def verify_event_path(self, now: float, tp: TP) -> None:
        ...

    def verify_recovery_event_path(self, now: float, tp: TP) -> None:
        ...

    async def commit(self, topics: TPorTopicSet = None,
                     start_new_transaction: bool = True) -> bool:
        """Maybe commit the offset for all or specific topics.

        Arguments:
            topics: Set containing topics and/or TopicPartitions to commit.
        """
        pass

    async def maybe_wait_for_commit_to_finish(self) -> bool:
        """Wait for any existing commit operation to finish."""
        pass

    @Service.transitions_to(CONSUMER_COMMITTING)
    async def force_commit(self,
                           topics: TPorTopicSet = None,
                           start_new_transaction: bool = True) -> bool:
        """Force offset commit."""
        pass

    async def _commit_tps(self,
                          tps: Iterable[TP],
                          start_new_transaction: bool) -> bool:
        pass

    def _filter_committable_offsets(self, tps: Iterable[TP]) -> Dict[TP, int]:
        pass

    async def _handle_attached(self, commit_offsets: Mapping[TP, int]) -> None:
        pass

    async def _commit_offsets(self, offsets: Mapping[TP, int],
                              start_new_transaction: bool = True) -> bool:
        pass

    def _filter_tps_with_pending_acks(
            self, topics: TPorTopicSet = None) -> Iterator[TP]:
        pass

    def _should_commit(self, tp: TP, offset: int) -> bool:
        pass

    def _new_offset(self, tp: TP) -> Optional[int]:
        # get the new offset for this tp, by going through
        # its list of acked messages.
        pass

    async def on_task_error(self, exc: BaseException) -> None:
        """Call when processing a message failed."""
        pass

    def _add_gap(self, tp: TP, offset_from: int, offset_to: int) -> None:
        pass

    async def _drain_messages(
            self, fetcher: ServiceT) -> None:  # pragma: no cover
        # This is the background thread started by Fetcher, used to
        # constantly read messages using Consumer.getmany.
        # It takes Fetcher as argument, because we must be able to
        # stop it using `await Fetcher.stop()`.
        pass

    def close(self) -> None:
        """Close consumer for graceful shutdown."""
        ...

    @property
    def unacked(self) -> Set[Message]:
        """Return the set of currently unacknowledged messages."""
        pass


class ConsumerThread(QueueServiceThread):
    """Consumer running in a dedicated thread."""

    app: AppT
    consumer: 'ThreadDelegateConsumer'
    transport: TransportT

    def __init__(self, consumer: 'ThreadDelegateConsumer',
                 **kwargs: Any) -> None:
        self.consumer = consumer
        self.transport = self.consumer.transport
        self.app = self.transport.app
        super().__init__(**kwargs)

    @abc.abstractmethod
    async def subscribe(self, topics: Iterable[str]) -> None:
        """Reset subscription (requires rebalance)."""
        ...

    @abc.abstractmethod
    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek all partitions to their committed offsets."""
        ...

    @abc.abstractmethod
    async def commit(self, tps: Mapping[TP, int]) -> bool:
        """Commit offsets in topic partitions."""
        ...

    @abc.abstractmethod
    async def position(self, tp: TP) -> Optional[int]:
        """Return the current offset for partition."""
        ...

    @abc.abstractmethod
    async def seek_to_beginning(self, *partitions: TP) -> None:
        """Seek to the earliest offsets available for partitions."""
        ...

    @abc.abstractmethod
    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        """Seek partitions to specific offsets and wait."""
        ...

    @abc.abstractmethod
    def seek(self, partition: TP, offset: int) -> None:
        """Seek partition to specific offset."""
        ...

    @abc.abstractmethod
    def assignment(self) -> Set[TP]:
        """Return the current assignment."""
        ...

    @abc.abstractmethod
    def highwater(self, tp: TP) -> int:
        """Return the last available offset in partition."""
        ...

    @abc.abstractmethod
    def topic_partitions(self, topic: str) -> Optional[int]:
        """Return number of configured partitions for topic by name."""
        ...

    @abc.abstractmethod
    def close(self) -> None:
        ...

    @abc.abstractmethod
    async def earliest_offsets(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the earliest available offset for list of partitions."""
        ...

    @abc.abstractmethod
    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the last available offset for list of partitions."""
        ...

    @abc.abstractmethod
    async def getmany(self,
                      active_partitions: Optional[Set[TP]],
                      timeout: float) -> RecordMap:
        """Fetch batch of messages from server."""
        ...

    @abc.abstractmethod
    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 30.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        ...

    async def on_partitions_revoked(
            self, revoked: Set[TP]) -> None:
        """Call on rebalance when partitions are being revoked."""
        pass

    async def on_partitions_assigned(
            self, assigned: Set[TP]) -> None:
        """Call on rebalance when partitions are being assigned."""
        await self.consumer.threadsafe_partitions_assigned(
            self.thread_loop, assigned)

    @abc.abstractmethod
    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> Optional[int]:
        """Hash key to determine partition number."""
        ...

    def verify_recovery_event_path(self, now: float, tp: TP) -> None:
        ...


class ThreadDelegateConsumer(Consumer):

    _thread: ConsumerThread

    #: Main thread method queue.
    #: The consumer is running in a separate thread, and so we send
    #: requests to it via a queue.
    #: Sometimes the thread needs to call code owned by the main thread,
    #: such as App.on_partitions_revoked, and in that case the thread
    #: uses this method queue.
    _method_queue: MethodQueue

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._method_queue = MethodQueue(loop=self.loop, beacon=self.beacon)
        self.add_dependency(self._method_queue)
        self._thread = self._new_consumer_thread()
        self.add_dependency(self._thread)

    @abc.abstractmethod
    def _new_consumer_thread(self) -> ConsumerThread:
        ...

    async def threadsafe_partitions_revoked(
            self,
            receiver_loop: asyncio.AbstractEventLoop,
            revoked: Set[TP]) -> None:
        """Call rebalancing callback in a thread-safe manner."""
        pass

    async def threadsafe_partitions_assigned(
            self,
            receiver_loop: asyncio.AbstractEventLoop,
            assigned: Set[TP]) -> None:
        """Call rebalancing callback in a thread-safe manner."""
        promise = await self._method_queue.call(
            receiver_loop.create_future(),
            self.on_partitions_assigned,
            assigned,
        )
        # wait for main-thread to finish processing request
        await promise

    async def _getmany(self,
                       active_partitions: Optional[Set[TP]],
                       timeout: float) -> RecordMap:
        pass

    async def subscribe(self, topics: Iterable[str]) -> None:
        """Reset subscription (requires rebalance)."""
        pass

    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek all partitions to the committed offset."""
        pass

    async def position(self, tp: TP) -> Optional[int]:
        """Return the current position for partition."""
        return await self._thread.position(tp)

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        """Seek partitions to specific offsets and wait."""
        pass

    async def _seek(self, partition: TP, offset: int) -> None:
        pass

    def assignment(self) -> Set[TP]:
        """Return the current assignment."""
        return self._thread.assignment()

    def highwater(self, tp: TP) -> int:
        """Return the last available offset for specific partition."""
        return self._thread.highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        """Return the number of partitions configured for topic by name."""
        pass

    async def earliest_offsets(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the earliest offsets for a list of partitions."""
        pass

    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the last offset for a list of partitions."""
        pass

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        pass

    def close(self) -> None:
        """Close consumer for graceful shutdown."""
        pass

    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> Optional[int]:
        """Hash key to determine partition number."""
        return self._thread.key_partition(topic, key, partition=partition)

    def verify_recovery_event_path(self, now: float, tp: TP) -> None:
        pass
