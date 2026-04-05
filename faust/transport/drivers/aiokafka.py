"""Message transport using :pypi:`aiokafka`."""
import asyncio
import typing

from collections import deque
from time import monotonic
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Set,
    Tuple,
    Type,
    cast,
    no_type_check,
)

import aiokafka
import aiokafka.abc
import opentracing
from aiokafka.consumer.group_coordinator import OffsetCommitRequest
from aiokafka.errors import (
    CommitFailedError,
    ConsumerStoppedError,
    IllegalStateError,
    KafkaError,
)
from aiokafka.structs import (
    OffsetAndMetadata,
    TopicPartition as _TopicPartition,
)
from aiokafka.util import parse_kafka_version
from kafka.errors import (
    NotControllerError,
    TopicAlreadyExistsError as TopicExistsError,
    for_code,
)
from kafka.partitioner.default import DefaultPartitioner
from kafka.partitioner.hashed import murmur2
from kafka.protocol.metadata import MetadataRequest_v1
from mode import Service, get_logger
from mode.utils.futures import StampedeWrapper
from mode.utils.objects import cached_property
from mode.utils import text
from mode.utils.times import Seconds, humanize_seconds_ago, want_seconds
from mode.utils.typing import Deque
from opentracing.ext import tags
from yarl import URL

from faust.auth import (
    GSSAPICredentials,
    SASLCredentials,
    SSLCredentials,
)
from faust.exceptions import (
    ConsumerNotStarted,
    ImproperlyConfigured,
    NotReady,
    ProducerSendError,
)
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TPset,
)
from faust.types import ConsumerMessage, HeadersArg, RecordMetadata, TP
from faust.types.auth import CredentialsT
from faust.types.transports import (
    ConsumerT,
    PartitionerT,
    ProducerT,
)
from faust.utils.kafka.protocol.admin import CreateTopicsRequest
from faust.utils.tracing import (
    noop_span,
    set_current_span,
    traced_from_parent_span,
)

__all__ = ['Consumer', 'Producer', 'Transport']

if not hasattr(aiokafka, '__robinhood__'):  # pragma: no cover
    raise RuntimeError(
        'Please install robinhood-aiokafka, not aiokafka')

logger = get_logger(__name__)

DEFAULT_GENERATION_ID = OffsetCommitRequest.DEFAULT_GENERATION_ID

TOPIC_LENGTH_MAX = 249

SLOW_PROCESSING_CAUSE_AGENT = '''
The agent processing the stream is hanging (waiting for network, I/O or \
infinite loop).
'''.strip()

SLOW_PROCESSING_CAUSE_STREAM = '''
The stream has stopped processing events for some reason.
'''.strip()


SLOW_PROCESSING_CAUSE_COMMIT = '''
The commit handler background thread has stopped working (report as bug).
'''.strip()


SLOW_PROCESSING_EXPLAINED = '''

There are multiple possible explanations for this:

1) The processing of a single event in the stream
   is taking too long.

    The timeout for this is defined by the %(setting)s setting,
    currently set to %(current_value)r.  If you expect the time
    required to process an event, to be greater than this then please
    increase the timeout.

'''

SLOW_PROCESSING_NO_FETCH_SINCE_START = '''
Aiokafka has not sent fetch request for %r since start (started %s)
'''.strip()

SLOW_PROCESSING_NO_RESPONSE_SINCE_START = '''
Aiokafka has not received fetch response for %r since start (started %s)
'''.strip()

SLOW_PROCESSING_NO_RECENT_FETCH = '''
Aiokafka stopped fetching from %r (last done %s)
'''.strip()

SLOW_PROCESSING_NO_RECENT_RESPONSE = '''
Broker stopped responding to fetch requests for %r (last responded %s)
'''.strip()

SLOW_PROCESSING_NO_HIGHWATER_SINCE_START = '''
Highwater not yet available for %r (started %s).
'''.strip()

SLOW_PROCESSING_STREAM_IDLE_SINCE_START = '''
Stream has not started processing %r (started %s).
'''.strip()

SLOW_PROCESSING_STREAM_IDLE = '''
Stream stopped processing, or is slow for %r (last inbound %s).
'''.strip()

SLOW_PROCESSING_NO_COMMIT_SINCE_START = '''
Has not committed %r at all since worker start (started %s).
'''.strip()

SLOW_PROCESSING_NO_RECENT_COMMIT = '''
Has not committed %r (last commit %s).
'''.strip()


def server_list(urls: List[URL], default_port: int) -> List[str]:
    """Convert list of urls to list of servers accepted by :pypi:`aiokafka`."""
    default_host = '127.0.0.1'
    return [f'{u.host or default_host}:{u.port or default_port}' for u in urls]


class ConsumerRebalanceListener(
        aiokafka.abc.ConsumerRebalanceListener):  # type: ignore
    # kafka's ridiculous class based callback interface makes this hacky.

    def __init__(self, thread: ConsumerThread) -> None:
        self._thread: ConsumerThread = thread

    def on_partitions_revoked(
            self, revoked: Iterable[_TopicPartition]) -> Awaitable:
        """Call when partitions are being revoked."""
        pass

    async def on_partitions_assigned(
            self, assigned: Iterable[_TopicPartition]) -> None:
        """Call when partitions are being assigned."""
        await self._thread.on_partitions_assigned(ensure_TPset(assigned))


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`aiokafka`."""

    logger = logger

    RebalanceListener: ClassVar[Type[ConsumerRebalanceListener]]
    RebalanceListener = ConsumerRebalanceListener

    consumer_stopped_errors: ClassVar[Tuple[Type[BaseException], ...]] = (
        ConsumerStoppedError,
    )

    def _new_consumer_thread(self) -> ConsumerThread:
        return AIOKafkaConsumerThread(self, loop=self.loop, beacon=self.beacon)

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
        await self._thread.create_topic(
            topic,
            partitions,
            replication,
            config=config,
            timeout=timeout,
            retention=retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        pass

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        pass

    async def on_stop(self) -> None:
        """Call when consumer is stopping."""
        pass


class AIOKafkaConsumerThread(ConsumerThread):
    _consumer: Optional[aiokafka.AIOKafkaConsumer] = None
    _pending_rebalancing_spans: Deque[opentracing.Span]

    tp_last_committed_at: MutableMapping[TP, float]
    time_started: float

    tp_fetch_request_timeout_secs: float
    tp_fetch_response_timeout_secs: float
    tp_stream_timeout_secs: float
    tp_commit_timeout_secs: float

    def __post_init__(self) -> None:
        consumer = cast(Consumer, self.consumer)
        self._partitioner: PartitionerT = (
            self.app.conf.producer_partitioner or DefaultPartitioner())
        self._rebalance_listener = consumer.RebalanceListener(self)
        self._pending_rebalancing_spans = deque()
        self.tp_last_committed_at = {}

        app = self.consumer.app

        stream_processing_timeout = app.conf.stream_processing_timeout
        self.tp_fetch_request_timeout_secs = stream_processing_timeout
        self.tp_fetch_response_timeout_secs = stream_processing_timeout
        self.tp_stream_timeout_secs = stream_processing_timeout

        commit_livelock_timeout = app.conf.broker_commit_livelock_soft_timeout
        self.tp_commit_timeout_secs = commit_livelock_timeout

    async def on_start(self) -> None:
        """Call when consumer starts."""
        pass

    async def on_thread_stop(self) -> None:
        """Call when consumer thread is stopping."""
        pass

    def _create_consumer(
            self,
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        pass

    def _create_worker_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        pass

    def _create_client_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> aiokafka.AIOKafkaConsumer:
        pass

    @cached_property
    def trace_category(self) -> str:
        pass

    def start_rebalancing_span(self) -> opentracing.Span:
        pass

    def start_coordinator_span(self) -> opentracing.Span:
        pass

    def _start_span(self, name: str, *,
                    lazy: bool = False) -> opentracing.Span:
        pass

    @no_type_check
    def _transform_span_lazy(self, span: opentracing.Span) -> None:
        # XXX slow
        pass

    def _span_finish(self, span: opentracing.Span) -> None:
        pass

    def _on_span_generation_pending(self, span: opentracing.Span) -> None:
        pass

    def _on_span_generation_known(self, span: opentracing.Span) -> None:
        pass

    def _on_span_cancelled_early(self, span: opentracing.Span) -> None:
        pass

    def traced_from_parent_span(self,
                                parent_span: opentracing.Span,
                                lazy: bool = False,
                                **extra_context: Any) -> Callable:
        return traced_from_parent_span(
            parent_span,
            callback=self._transform_span_lazy if lazy else None,
            **extra_context)

    def flush_spans(self) -> None:
        pass

    def on_generation_id_known(self) -> None:
        pass

    def close(self) -> None:
        """Close consumer for graceful shutdown."""
        pass

    async def subscribe(self, topics: Iterable[str]) -> None:
        """Reset subscription (requires rebalance)."""
        pass

    async def seek_to_committed(self) -> Mapping[TP, int]:
        """Seek partitions to the last committed offset."""
        pass

    async def commit(self, offsets: Mapping[TP, int]) -> bool:
        """Commit topic offsets."""
        pass

    async def _commit(self, offsets: Mapping[TP, int]) -> bool:
        pass

    def verify_event_path(self, now: float, tp: TP) -> None:
        # long function ahead, but not difficult to test
        # as it always returns as soon as some condition is met.
        pass

    def verify_recovery_event_path(self, now: float, tp: TP) -> None:
        pass

    def _verify_aiokafka_event_path(self, now: float, tp: TP) -> bool:
        """Verify that :pypi:`aiokafka` event path is working.

        Returns :const:`True` if any error was logged.
        """
        pass

    def _log_slow_processing_stream(self, msg: str, *args: Any) -> None:
        pass

    def _log_slow_processing_commit(self, msg: str, *args: Any) -> None:
        pass

    def _make_slow_processing_error(self,
                                    msg: str,
                                    causes: Iterable[str]) -> str:
        pass

    def _log_slow_processing(self, msg: str, *args: Any,
                             causes: Iterable[str],
                             setting: str,
                             current_value: float) -> None:
        pass

    async def position(self, tp: TP) -> Optional[int]:
        """Return the current position for topic partition."""
        return await self.call_thread(
            self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        """Seek list of offsets to the first available offset."""
        pass

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        """Seek partitions to specific offset and wait for operation."""
        pass

    async def _seek_wait(self,
                         consumer: Consumer,
                         partitions: Mapping[TP, int]) -> None:
        pass

    def seek(self, partition: TP, offset: int) -> None:
        """Seek partition to specific offset."""
        pass

    def assignment(self) -> Set[TP]:
        """Return the current assignment."""
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        """Return the last offset in a specific partition."""
        if self.consumer.in_transaction:
            return self._ensure_consumer().last_stable_offset(tp)
        else:
            return self._ensure_consumer().highwater(tp)

    def topic_partitions(self, topic: str) -> Optional[int]:
        """Return the number of partitions configured for topic by name."""
        pass

    async def earliest_offsets(self,
                               *partitions: TP) -> Mapping[TP, int]:
        """Return the earliest offsets for a list of partitions."""
        pass

    async def highwaters(self, *partitions: TP) -> Mapping[TP, int]:
        """Return the last offsets for a list of partitions."""
        pass

    async def _highwaters(self, partitions: List[TP]) -> Mapping[TP, int]:
        consumer = self._ensure_consumer()
        if self.consumer.in_transaction:
            return {
                tp: consumer.last_stable_offset(tp)
                for tp in partitions
            }
        else:
            return cast(Mapping[TP, int],
                        await consumer.end_offsets(partitions))

    def _ensure_consumer(self) -> aiokafka.AIOKafkaConsumer:
        if self._consumer is None:
            raise ConsumerNotStarted('Consumer thread not yet started')
        return self._consumer

    async def getmany(self,
                      active_partitions: Optional[Set[TP]],
                      timeout: float) -> RecordMap:
        """Fetch batch of messages from server."""
        pass

    async def _fetch_records(self,
                             consumer: aiokafka.AIOKafkaConsumer,
                             active_partitions: Set[TP],
                             timeout: float = None,
                             max_records: int = None) -> RecordMap:
        pass

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
        transport = cast(Transport, self.consumer.transport)
        _consumer = self._ensure_consumer()
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        if len(topic) > TOPIC_LENGTH_MAX:
            raise ValueError(
                f'Topic name {topic!r} is too long (max={TOPIC_LENGTH_MAX})')
        await self.call_thread(
            transport._create_topic,
            self,
            _consumer._client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> Optional[int]:
        """Hash key to determine partition destination."""
        consumer = self._ensure_consumer()
        metadata = consumer._client.cluster
        partitions_for_topic = metadata.partitions_for_topic(topic)
        if partitions_for_topic is None:
            return None
        if partition is not None:
            assert partition >= 0
            assert partition in partitions_for_topic, \
                'Unrecognized partition'
            return partition

        all_partitions = list(partitions_for_topic)
        available = list(metadata.available_partitions_for_topic(topic))
        return self._partitioner(key, all_partitions, available)


class Producer(base.Producer):
    """Kafka producer using :pypi:`aiokafka`."""

    logger = logger

    allow_headers: bool = True
    _producer: Optional[aiokafka.AIOKafkaProducer] = None

    def __post_init__(self) -> None:
        self._send_on_produce_message = self.app.on_produce_message.send
        if self.partitioner is None:
            self.partitioner = DefaultPartitioner()
        if self._api_version != 'auto':
            wanted_api_version = parse_kafka_version(self._api_version)
            if wanted_api_version < (0, 11):
                self.allow_headers = False

    def _settings_default(self) -> Mapping[str, Any]:
        transport = cast(Transport, self.transport)
        return {
            'bootstrap_servers': server_list(
                transport.url, transport.default_port),
            'client_id': self.client_id,
            'acks': self.acks,
            'linger_ms': self.linger_ms,
            'max_batch_size': self.max_batch_size,
            'max_request_size': self.max_request_size,
            'compression_type': self.compression_type,
            'on_irrecoverable_error': self._on_irrecoverable_error,
            'security_protocol': 'SSL' if self.ssl_context else 'PLAINTEXT',
            'partitioner': self.partitioner,
            'request_timeout_ms': int(self.request_timeout * 1000),
            'api_version': self._api_version,
        }

    def _settings_auth(self) -> Mapping[str, Any]:
        return credentials_to_aiokafka_auth(
            self.credentials, self.ssl_context)

    async def begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction by id."""
        pass

    async def commit_transaction(self, transactional_id: str) -> None:
        """Commit transaction by id."""
        pass

    async def abort_transaction(self, transactional_id: str) -> None:
        """Abort and rollback transaction by id."""
        pass

    async def stop_transaction(self, transactional_id: str) -> None:
        """Stop transaction by id."""
        pass

    async def maybe_begin_transaction(self, transactional_id: str) -> None:
        """Begin transaction (if one does not already exist)."""
        pass

    async def commit_transactions(
            self,
            tid_to_offset_map: Mapping[str, Mapping[TP, int]],
            group_id: str,
            start_new_transaction: bool = True) -> None:
        """Commit transactions."""
        pass

    def _settings_extra(self) -> Mapping[str, Any]:
        if self.app.in_transaction:
            return {'acks': 'all'}
        return {}

    def _new_producer(self) -> aiokafka.AIOKafkaProducer:
        return self._producer_type(
            loop=self.loop,
            **{**self._settings_default(),
               **self._settings_auth(),
               **self._settings_extra()},
        )

    @property
    def _producer_type(self) -> Type[aiokafka.BaseProducer]:
        if self.app.in_transaction:
            return aiokafka.MultiTXNProducer
        return aiokafka.AIOKafkaProducer

    async def _on_irrecoverable_error(self, exc: BaseException) -> None:
        pass

    async def create_topic(self,
                           topic: str,
                           partitions: int,
                           replication: int,
                           *,
                           config: Mapping[str, Any] = None,
                           timeout: Seconds = 20.0,
                           retention: Seconds = None,
                           compacting: bool = None,
                           deleting: bool = None,
                           ensure_created: bool = False) -> None:
        """Create/declare topic on server."""
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        producer = self._ensure_producer()
        await cast(Transport, self.transport)._create_topic(
            self,
            producer.client,
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=_retention,
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )
        await producer.client.force_metadata_update()  # Fixes #499

    def _ensure_producer(self) -> aiokafka.BaseProducer:
        if self._producer is None:
            raise NotReady('Producer service not yet started')
        return self._producer

    async def on_start(self) -> None:
        """Call when producer starts."""
        pass

    async def on_stop(self) -> None:
        """Call when producer stops."""
        pass

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        """Schedule message to be transmitted by producer."""
        producer = self._ensure_producer()
        if headers is not None:
            if isinstance(headers, Mapping):
                headers = list(headers.items())
        self._send_on_produce_message(
            key=key, value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
        )
        if headers is not None and not self.allow_headers:
            headers = None
        timestamp_ms = int(timestamp * 1000.0) if timestamp else timestamp
        try:
            return cast(Awaitable[RecordMetadata], await producer.send(
                topic, value,
                key=key,
                partition=partition,
                timestamp_ms=timestamp_ms,
                headers=headers,
                transactional_id=transactional_id,
            ))
        except KafkaError as exc:
            raise ProducerSendError(f'Error while sending: {exc!r}') from exc

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        """Send message and wait for it to be transmitted."""
        fut = await self.send(
            topic,
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            transactional_id=transactional_id,
        )
        return await fut

    async def flush(self) -> None:
        """Wait for producer to finish transmitting all buffered messages."""
        await self.buffer.flush()
        if self._producer is not None:
            await self._producer.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Hash key to determine partition destination."""
        producer = self._ensure_producer()
        partition = producer._partition(
            topic,
            partition=None,
            key=None,
            value=None,
            serialized_key=key,
            serialized_value=None,
        )
        return TP(topic, partition)

    def supports_headers(self) -> bool:
        """Return :const:`True` if message headers are supported."""
        pass


class Transport(base.Transport):
    """Kafka transport using :pypi:`aiokafka`."""

    Consumer: ClassVar[Type[ConsumerT]]
    Consumer = Consumer
    Producer: ClassVar[Type[ProducerT]]
    Producer = Producer

    default_port = 9092
    driver_version = f'aiokafka={aiokafka.__version__}'

    _topic_waiters: MutableMapping[str, StampedeWrapper]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._topic_waiters = {}

    def _topic_config(self,
                      retention: int = None,
                      compacting: bool = None,
                      deleting: bool = None) -> MutableMapping[str, Any]:
        pass

    async def _create_topic(self,
                            owner: Service,
                            client: aiokafka.AIOKafkaClient,
                            topic: str,
                            partitions: int,
                            replication: int,
                            **kwargs: Any) -> None:
        assert topic is not None
        try:
            wrap = self._topic_waiters[topic]
        except KeyError:
            wrap = self._topic_waiters[topic] = StampedeWrapper(
                self._really_create_topic,
                owner,
                client,
                topic,
                partitions,
                replication,
                loop=asyncio.get_event_loop(), **kwargs)
        try:
            await wrap()
        except Exception:
            self._topic_waiters.pop(topic, None)
            raise

    async def _get_controller_node(
            self,
            owner: Service,
            client: aiokafka.AIOKafkaClient,
            timeout: int = 30000) -> Optional[int]:  # pragma: no cover
        pass

    async def _really_create_topic(
            self,
            owner: Service,
            client: aiokafka.AIOKafkaClient,
            topic: str,
            partitions: int,
            replication: int,
            *,
            config: Mapping[str, Any] = None,
            timeout: int = 30000,
            retention: int = None,
            compacting: bool = None,
            deleting: bool = None,
            ensure_created: bool = False) -> None:  # pragma: no cover
        pass


def credentials_to_aiokafka_auth(credentials: CredentialsT = None,
                                 ssl_context: Any = None) -> Mapping:
    if credentials is not None:
        if isinstance(credentials, SSLCredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'ssl_context': credentials.context,
            }
        elif isinstance(credentials, SASLCredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'sasl_mechanism': credentials.mechanism.value,
                'sasl_plain_username': credentials.username,
                'sasl_plain_password': credentials.password,
                'ssl_context': credentials.ssl_context,
            }
        elif isinstance(credentials, GSSAPICredentials):
            return {
                'security_protocol': credentials.protocol.value,
                'sasl_mechanism': credentials.mechanism.value,
                'sasl_kerberos_service_name':
                    credentials.kerberos_service_name,
                'sasl_kerberos_domain_name':
                    credentials.kerberos_domain_name,
                'ssl_context': credentials.ssl_context,
            }
        else:
            raise ImproperlyConfigured(
                f'aiokafka does not support {credentials}')
    elif ssl_context is not None:
        return {
            'security_protocol': 'SSL',
            'ssl_context': ssl_context,
        }
    else:
        return {'security_protocol': 'PLAINTEXT'}
