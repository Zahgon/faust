"""Streams."""
import asyncio
import os
import reprlib
import typing
import weakref

from asyncio import CancelledError
from contextvars import ContextVar
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from mode import Seconds, Service, get_logger, shortlabel, want_seconds
from mode.utils.aiter import aenumerate, aiter
from mode.utils.futures import current_task, maybe_async, notify
from mode.utils.queues import ThrowableQueue
from mode.utils.typing import Deque
from mode.utils.types.trees import NodeT

from . import joins
from .exceptions import ImproperlyConfigured, Skip
from .types import AppT, ConsumerT, EventT, K, ModelArg, ModelT, TP, TopicT
from .types.joins import JoinT
from .types.models import FieldDescriptorT
from .types.serializers import SchemaT
from .types.streams import (
    GroupByKeyArg,
    JoinableT,
    Processor,
    StreamT,
    T,
    T_co,
    T_contra,
)
from .types.topics import ChannelT
from .types.tuples import Message

NO_CYTHON = bool(os.environ.get('NO_CYTHON', False))

if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.streams import StreamIterator as _CStreamIterator
    except ImportError:
        _CStreamIterator = None
else:  # pragma: no cover
    _CStreamIterator = None

__all__ = [
    'Stream',
    'current_event',
]

logger = get_logger(__name__)

if typing.TYPE_CHECKING:  # pragma: no cover
    _current_event: ContextVar[Optional[weakref.ReferenceType[EventT]]]
_current_event = ContextVar('current_event')


def current_event() -> Optional[EventT]:
    """Return the event currently being processed, or None."""
    eventref = _current_event.get(None)
    return eventref() if eventref is not None else None


async def maybe_forward(value: Any, channel: ChannelT) -> Any:
    if isinstance(value, EventT):
        await value.forward(channel)
    else:
        await channel.send(value=value)
    return value


class _LinkedListDirection(NamedTuple):
    attr: str
    getter: Callable[[StreamT], Optional[StreamT]]


_LinkedListDirectionFwd = _LinkedListDirection('_next', lambda n: n._next)
_LinkedListDirectionBwd = _LinkedListDirection('_prev', lambda n: n._prev)


class Stream(StreamT[T_co], Service):
    """A stream: async iterator processing events in channels/topics."""

    logger = logger
    # Service starting/stopping logs use severity DEBUG in this class.
    mundane_level = 'debug'

    #: Number of events processed by this instance so far.
    events_total: int = 0

    _processors: MutableSequence[Processor]
    _anext_started = False
    _passive = False
    _finalized = False
    _passive_started: asyncio.Event

    def __init__(self,
                 channel: AsyncIterator[T_co],
                 *,
                 app: AppT,
                 processors: Iterable[Processor[T]] = None,
                 combined: List[JoinableT] = None,
                 on_start: Callable = None,
                 join_strategy: JoinT = None,
                 beacon: NodeT = None,
                 concurrency_index: int = None,
                 prev: StreamT = None,
                 active_partitions: Set[TP] = None,
                 enable_acks: bool = True,
                 prefix: str = '',
                 loop: asyncio.AbstractEventLoop = None) -> None:
        Service.__init__(self, loop=loop, beacon=beacon)
        self.app = app
        self.channel = channel
        self.outbox = self.app.FlowControlQueue(
            maxsize=self.app.conf.stream_buffer_maxsize,
            loop=self.loop,
            clear_on_resume=True,
        )
        self._passive_started = asyncio.Event(loop=self.loop)
        self.join_strategy = join_strategy
        self.combined = combined if combined is not None else []
        self.concurrency_index = concurrency_index
        self._prev = prev
        self.active_partitions = active_partitions
        self.enable_acks = enable_acks
        self.prefix = prefix

        self._processors = list(processors) if processors else []
        self._on_start = on_start

        # attach beacon to channel, or if iterable attach to current task.
        task = current_task(loop=self.loop)
        if task is not None:
            self.task_owner = task

        # Generate message handler
        self._on_stream_event_in = self.app.sensors.on_stream_event_in
        self._on_stream_event_out = self.app.sensors.on_stream_event_out
        self._on_message_in = self.app.sensors.on_message_in
        self._on_message_out = self.app.sensors.on_message_out

        self._skipped_value = object()

    def get_active_stream(self) -> StreamT:
        """Return the currently active stream.

        A stream can be derived using ``Stream.group_by`` etc,
        so if this stream was used to create another derived
        stream, this function will return the stream being actively
        consumed from.  E.g. in the example::

            >>> @app.agent()
            ... async def agent(a):
            ..      a = a
            ...     b = a.group_by(Withdrawal.account_id)
            ...     c = b.through('backup_topic')
            ...     async for value in c:
            ...         ...

        The return value of ``a.get_active_stream()`` would be ``c``.

        Notes:
            The chain of streams that leads to the active stream
            is decided by the :attr:`_next` attribute. To get
            to the active stream we just traverse this linked-list::

                >>> def get_active_stream(self):
                ...     node = self
                ...     while node._next:
                ...         node = node._next
        """
        return list(self._iter_ll_forwards())[-1]

    def get_root_stream(self) -> StreamT:
        """Get the root stream that this stream was derived from."""
        return list(self._iter_ll_backwards())[-1]

    def _iter_ll_forwards(self) -> Iterator[StreamT]:
        return self._iter_ll(_LinkedListDirectionFwd)

    def _iter_ll_backwards(self) -> Iterator[StreamT]:
        return self._iter_ll(_LinkedListDirectionBwd)

    def _iter_ll(self, dir_: _LinkedListDirection) -> Iterator[StreamT]:
        node: Optional[StreamT] = self
        seen: Set[StreamT] = set()
        while node:
            if node in seen:
                raise RuntimeError(
                    'Loop in Stream.{dir_.attr}: Call support!')
            seen.add(node)
            yield node
            node = dir_.getter(node)

    def add_processor(self, processor: Processor[T]) -> None:
        """Add processor callback executed whenever a new event is received.

        Processor functions can be async or non-async, must accept
        a single argument, and should return the value, mutated or not.

        For example a processor handling a stream of numbers may modify
        the value::

            def double(value: int) -> int:
                return value * 2

            stream.add_processor(double)
        """
        self._processors.append(processor)

    def info(self) -> Mapping[str, Any]:
        """Return stream settings as a dictionary."""
        # used by e.g. .clone to reconstruct keyword arguments
        # needed to create a clone of the stream.
        return {
            'app': self.app,
            'channel': self.channel,
            'processors': self._processors,
            'on_start': self._on_start,
            'loop': self.loop,
            'combined': self.combined,
            'beacon': self.beacon,
            'concurrency_index': self.concurrency_index,
            'prev': self._prev,
            'active_partitions': self.active_partitions,
        }

    def clone(self, **kwargs: Any) -> StreamT:
        """Create a clone of this stream.

        Notes:
            If the cloned stream is supposed to supersede this stream,
            like in ``group_by``/``through``/etc., you should use
            :meth:`_chain` instead so `stream._next = cloned_stream`
            is set and :meth:`get_active_stream` returns the cloned stream.
        """
        return self.__class__(**{**self.info(), **kwargs})

    def _chain(self, **kwargs: Any) -> StreamT:
        pass

    def noack(self) -> 'StreamT':
        """Create new stream where acks are manual."""
        pass

    async def items(self) -> AsyncIterator[Tuple[K, T_co]]:
        """Iterate over the stream as ``key, value`` pairs.

        Examples:
            .. sourcecode:: python

                @app.agent(topic)
                async def mytask(stream):
                    async for key, value in stream.items():
                        print(key, value)
        """
        async for event in self.events():
            yield event.key, cast(T_co, event.value)

    async def events(self) -> AsyncIterable[EventT]:
        """Iterate over the stream as events exclusively.

        This means the stream must be iterating over a channel,
        or at least an iterable of event objects.
        """
        async for _ in self:  # noqa: F841
            if self.current_event is not None:
                yield self.current_event

    async def take(self, max_: int,
                   within: Seconds) -> AsyncIterable[Sequence[T_co]]:
        """Buffer n values at a time and yield a list of buffered values.

        Arguments:
            max_: Max number of messages to receive. When more than this
                number of messages are received within the specified number of
                seconds then we flush the buffer immediately.
            within: Timeout for when we give up waiting for another value,
                and process the values we have.
                Warning: If there's no timeout (i.e. `timeout=None`),
                the agent is likely to stall and block buffered events for an
                unreasonable length of time(!).
        """
        pass

    def enumerate(self, start: int = 0) -> AsyncIterable[Tuple[int, T_co]]:
        """Enumerate values received on this stream.

        Unlike Python's built-in ``enumerate``, this works with
        async generators.
        """
        pass

    def through(self, channel: Union[str, ChannelT]) -> StreamT:
        """Forward values to in this stream to channel.

        Send messages received on this stream to another channel,
        and return a new stream that consumes from that channel.

        Notes:
            The messages are forwarded after any processors have been
            applied.

        Example:
            .. sourcecode:: python

                topic = app.topic('foo')

                @app.agent(topic)
                async def mytask(stream):
                    async for value in stream.through(app.topic('bar')):
                        # value was first received in topic 'foo',
                        # then forwarded and consumed from topic 'bar'
                        print(value)
        """
        pass

    def _enable_passive(self, channel: ChannelT, *,
                        declare: bool = False) -> None:
        pass

    async def _passive_drainer(self, channel: ChannelT,
                               declare: bool = False) -> None:
        pass

    def _channel_stop_iteration(self, channel: Any) -> None:
        pass

    def echo(self, *channels: Union[str, ChannelT]) -> StreamT:
        """Forward values to one or more channels.

        Unlike :meth:`through`, we don't consume from these channels.
        """
        _channels = [
            self.derive_topic(c) if isinstance(c, str) else c for c in channels
        ]

        async def echoing(value: T) -> T:
            pass

        self.add_processor(echoing)
        return self

    def group_by(self,
                 key: GroupByKeyArg,
                 *,
                 name: str = None,
                 topic: TopicT = None,
                 partitions: int = None) -> StreamT:
        """Create new stream that repartitions the stream using a new key.

        Arguments:
            key: The key argument decides how the new key is generated,
                it can be a field descriptor, a callable, or an async
                callable.

                Note: The ``name`` argument must be provided if the key
                    argument is a callable.

            name: Suffix to use for repartitioned topics.
                This argument is required if `key` is a callable.

        Examples:
            Using a field descriptor to use a field in the event as the new
            key:

            .. sourcecode:: python

                s = withdrawals_topic.stream()
                # values in this stream are of type Withdrawal
                async for event in s.group_by(Withdrawal.account_id):
                    ...

            Using an async callable to extract a new key:

            .. sourcecode:: python

                s = withdrawals_topic.stream()

                async def get_key(withdrawal):
                    return await aiohttp.get(
                        f'http://e.com/resolve_account/{withdrawal.account_id}')

                async for event in s.group_by(get_key):
                    ...

            Using a regular callable to extract a new key:

            .. sourcecode:: python

                s = withdrawals_topic.stream()

                def get_key(withdrawal):
                    return withdrawal.account_id.upper()

                async for event in s.group_by(get_key):
                    ...
        """
        pass

    def filter(self, fun: Processor[T]) -> StreamT:
        """Filter values from stream using callback.

        The callback may be a traditional function, lambda function,
        or an `async def` function.

        This method is useful for filtering events before repartitioning
        a stream.

        Examples:
            >>> async for v in stream.filter(lambda: v > 1000).group_by(...):
            ...     # do something
        """
        pass

    async def _format_key(self, key: GroupByKeyArg, value: T_contra) -> str:
        pass

    def derive_topic(self,
                     name: str,
                     *,
                     schema: SchemaT = None,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
                     prefix: str = '',
                     suffix: str = '') -> TopicT:
        """Create Topic description derived from the K/V type of this stream.

        Arguments:
            name: Topic name.

            key_type: Specific key type to use for this topic.
                If not set, the key type of this stream will be used.
            value_type: Specific value type to use for this topic.
                If not set, the value type of this stream will be used.

        Raises:
            ValueError: if the stream channel is not a topic.
        """
        if isinstance(self.channel, TopicT):
            return cast(TopicT, self.channel).derive_topic(
                topics=[name],
                schema=schema,
                key_type=key_type,
                value_type=value_type,
                prefix=prefix,
                suffix=suffix,
            )
        raise ValueError('Cannot derive topic from non-topic channel.')

    async def throw(self, exc: BaseException) -> None:
        """Send exception to stream iteration."""
        await cast(ChannelT, self.channel).throw(exc)

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        """Combine streams and tables into joined stream."""
        pass

    def contribute_to_stream(self, active: StreamT) -> None:
        """Add stream as node in joined stream."""
        pass

    async def remove_from_stream(self, stream: StreamT) -> None:
        """Remove as node in a joined stream."""
        pass

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined."""
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by LEFT JOIN."""
        pass

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by INNER JOIN."""
        pass

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by OUTER JOIN."""
        pass

    def _join(self, join_strategy: JoinT) -> StreamT:
        return self.clone(join_strategy=join_strategy)

    async def on_merge(self, value: T = None) -> Optional[T]:
        """Signal called when an event is to be joined."""
        pass

    async def on_start(self) -> None:
        """Signal called when the stream starts."""
        pass

    async def stop(self) -> None:
        """Stop this stream."""
        # Stop all related streams (created by .through/.group_by/etc.)
        for s in cast(Stream, self.get_root_stream())._iter_ll_forwards():
            await Service.stop(cast(Service, s))

    async def on_stop(self) -> None:
        """Signal that the stream is stopping."""
        pass

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> T:
        raise NotImplementedError('Streams are asynchronous: use `async for`')

    def __aiter__(self) -> AsyncIterator[T_co]:  # pragma: no cover
        if _CStreamIterator is not None:
            return self._c_aiter()
        else:
            return self._py_aiter()

    async def _c_aiter(self) -> AsyncIterator[T_co]:  # pragma: no cover
        pass

    def _set_current_event(self, event: EventT = None) -> None:
        pass

    async def _py_aiter(self) -> AsyncIterator[T_co]:
        pass

    async def __anext__(self) -> T:  # pragma: no cover
        ...

    async def ack(self, event: EventT) -> bool:
        """Ack event.

        This will decrease the reference count of the event message by one,
        and when the reference count reaches zero, the worker will
        commit the offset so that the message will not be seen by a worker
        again.

        Arguments:
            event: Event to ack.
        """
        # WARNING: This function is duplicated in __aiter__
        last_stream_to_ack = event.ack()
        message = event.message
        tp = message.tp
        offset = message.offset
        self._on_stream_event_out(tp, offset, self, event)
        if last_stream_to_ack:
            self._on_message_out(tp, offset, message)
        return last_stream_to_ack

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def __copy__(self) -> Any:
        return self.clone()

    def _repr_info(self) -> str:
        pass

    @property
    def label(self) -> str:
        """Return description of stream, used in graphs and logs."""
        return f'{type(self).__name__}: {self._repr_channel()}'

    def _repr_channel(self) -> str:
        return reprlib.repr(self.channel)

    @property
    def shortlabel(self) -> str:
        """Return short description of stream."""
        # used for shortlabel(stream), which is used by statsd to generate ids
        # note: str(channel) returns topic name when it's a topic, so
        # this will be:
        #    "Channel: <ANON>", for channel or
        #    "Topic: withdrawals", for a topic.
        # statsd then uses that as part of the id.
        return f'Stream: {self._human_channel()}'

    def _human_channel(self) -> str:
        if self.combined:
            return '&'.join(s._human_channel() for s in self.combined)
        return f'{type(self.channel).__name__}: {self.channel}'
