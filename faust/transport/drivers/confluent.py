"""Message transport using :pypi:`confluent_kafka`."""
import asyncio
import typing

from collections import defaultdict
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
    Type,
    cast,
)

from mode import Service, get_logger
from mode.threads import QueueServiceThread
from mode.utils.futures import notify
from mode.utils.times import Seconds, want_seconds
from yarl import URL

from faust.exceptions import ConsumerNotStarted, ProducerSendError
from faust.transport import base
from faust.transport.consumer import (
    ConsumerThread,
    RecordMap,
    ThreadDelegateConsumer,
    ensure_TP,
    ensure_TPset,
)
from faust.types import AppT, ConsumerMessage, HeadersArg, RecordMetadata, TP
from faust.types.transports import ConsumerT, ProducerT

import confluent_kafka
from confluent_kafka import TopicPartition as _TopicPartition
from confluent_kafka import KafkaException

if typing.TYPE_CHECKING:
    from confluent_kafka import Consumer as _Consumer
    from confluent_kafka import Producer as _Producer
    from confluent_kafka import Message as _Message
else:
    class _Consumer: ...  # noqa
    class _Producer: ...  # noqa
    class _Message: ...   # noqa

__all__ = ['Consumer', 'Producer', 'Transport']


logger = get_logger(__name__)


def server_list(urls: List[URL], default_port: int) -> str:
    default_host = '127.0.0.1'
    return ','.join([
        f'{u.host or default_host}:{u.port or default_port}' for u in urls])


class Consumer(ThreadDelegateConsumer):
    """Kafka consumer using :pypi:`confluent_kafka`."""

    logger = logger

    def _new_consumer_thread(self) -> ConsumerThread:
        return ConfluentConsumerThread(
            self, loop=self.loop, beacon=self.beacon)

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
        """Create topic on broker."""
        return  # XXX
        await self._thread.create_topic(
            topic,
            partitions,
            replication,
            config=config,
            timeout=int(want_seconds(timeout) * 1000.0),
            retention=int(want_seconds(retention) * 1000.0),
            compacting=compacting,
            deleting=deleting,
            ensure_created=ensure_created,
        )

    def _to_message(self, tp: TP, record: Any) -> ConsumerMessage:
        # convert timestamp to seconds from int milliseconds.
        pass

    def _new_topicpartition(self, topic: str, partition: int) -> TP:
        pass


class ConfluentConsumerThread(ConsumerThread):
    """Thread managing underlying :pypi:`confluent_kafka` consumer."""

    _consumer: Optional[_Consumer] = None
    _assigned: bool = False

    async def on_start(self) -> None:
        pass

    def _create_consumer(
            self,
            loop: asyncio.AbstractEventLoop) -> _Consumer:
        pass

    def _create_worker_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> _Consumer:
        pass

    def _create_client_consumer(
            self,
            transport: 'Transport',
            loop: asyncio.AbstractEventLoop) -> _Consumer:
        pass

    def close(self) -> None:
        ...

    async def subscribe(self, topics: Iterable[str]) -> None:
        # XXX pattern does not work :/
        pass

    def _on_assign(self,
                   consumer: _Consumer,
                   assigned: List[_TopicPartition]) -> None:
        pass

    def _on_revoke(self,
                   consumer: _Consumer,
                   revoked: List[_TopicPartition]) -> None:
        pass

    async def seek_to_committed(self) -> Mapping[TP, int]:
        pass

    async def _seek_to_committed(self) -> Mapping[TP, int]:
        pass

    async def _committed_offsets(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        pass

    async def commit(self, tps: Mapping[TP, int]) -> bool:
        pass

    async def position(self, tp: TP) -> Optional[int]:
        return await self.call_thread(
            self._ensure_consumer().position, tp)

    async def seek_to_beginning(self, *partitions: _TopicPartition) -> None:
        pass

    async def seek_wait(self, partitions: Mapping[TP, int]) -> None:
        pass

    async def _seek_wait(self,
                         consumer: Consumer,
                         partitions: Mapping[TP, int]) -> None:
        pass

    def seek(self, partition: TP, offset: int) -> None:
        pass

    def assignment(self) -> Set[TP]:
        return ensure_TPset(self._ensure_consumer().assignment())

    def highwater(self, tp: TP) -> int:
        _, hw = self._ensure_consumer().get_watermark_offsets(
            _TopicPartition(tp.topic, tp.partition), cached=True)
        return hw

    def topic_partitions(self, topic: str) -> Optional[int]:
        # XXX NotImplemented
        pass

    async def earliest_offsets(self,
                               *partitions: TP) -> MutableMapping[TP, int]:
        pass

    async def _earliest_offsets(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        pass

    async def highwaters(self, *partitions: TP) -> MutableMapping[TP, int]:
        pass

    async def _highwaters(
            self, partitions: List[TP]) -> MutableMapping[TP, int]:
        consumer = self._ensure_consumer()
        return {
            tp: consumer.get_watermark_offsets(
                _TopicPartition(tp[0], tp[1]))[1]
            for tp in partitions
        }

    def _ensure_consumer(self) -> _Consumer:
        if self._consumer is None:
            raise ConsumerNotStarted('Consumer thread not yet started')
        return self._consumer

    async def getmany(self,
                      active_partitions: Optional[Set[TP]],
                      timeout: float) -> RecordMap:
        # Implementation for the Fetcher service.
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
        return  # XXX

    def key_partition(self,
                      topic: str,
                      key: Optional[bytes],
                      partition: int = None) -> Optional[int]:
        raise NotImplementedError('TODO')  # TODO XXX


class ProducerProduceFuture(asyncio.Future):

    def set_from_on_delivery(self,
                             err: Optional[BaseException],
                             msg: _Message) -> None:
        pass

    def message_to_metadata(self, message: _Message) -> RecordMetadata:
        pass


class ProducerThread(QueueServiceThread):
    """Thread managing underlying :pypi:`confluent_kafka` producer."""

    app: AppT
    producer: 'Producer'
    transport: 'Transport'
    _producer: Optional[_Producer] = None
    _flush_soon: Optional[asyncio.Future] = None

    def __init__(self, producer: 'Producer', **kwargs: Any) -> None:
        self.producer = producer
        self.transport = cast(Transport, self.producer.transport)
        self.app = self.transport.app
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        pass

    async def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    async def on_thread_stop(self) -> None:
        pass

    def produce(self, topic: str, key: bytes, value: bytes, partition: int,
                on_delivery: Callable) -> None:
        pass

    @Service.task
    async def _background_flush(self) -> None:
        pass


class Producer(base.Producer):
    """Kafka producer using :pypi:`confluent_kafka`."""

    logger = logger

    _producer_thread: ProducerThread
    _quick_produce: Any = None

    def __post_init__(self) -> None:
        self._producer_thread = ProducerThread(
            self, loop=self.loop, beacon=self.beacon)
        self._quick_produce = self._producer_thread.produce

    async def _on_irrecoverable_error(self, exc: BaseException) -> None:
        pass

    async def on_restart(self) -> None:
        """Call when producer is restarting."""
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
        """Create topic on broker."""
        return  # XXX
        _retention = (int(want_seconds(retention) * 1000.0)
                      if retention else None)
        await cast(Transport, self.transport)._create_topic(
            self,
            self._producer.client,
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

    async def on_start(self) -> None:
        """Call when producer is starting."""
        pass

    async def on_stop(self) -> None:
        """Call when producer is stopping."""
        pass

    async def send(self, topic: str, key: Optional[bytes],
                   value: Optional[bytes],
                   partition: Optional[int],
                   timestamp: Optional[float],
                   headers: Optional[HeadersArg],
                   *,
                   transactional_id: str = None) -> Awaitable[RecordMetadata]:
        """Send message for future delivery."""
        fut = ProducerProduceFuture(loop=self.loop)
        self._quick_produce(
            topic, value, key, partition,
            timestamp=int(timestamp * 1000) if timestamp else timestamp,
            on_delivery=fut.set_from_on_delivery,
        )
        return cast(Awaitable[RecordMetadata], fut)
        try:
            return cast(Awaitable[RecordMetadata], await self._producer.send(
                topic, value, key=key, partition=partition))
        except KafkaException as exc:
            raise ProducerSendError(f'Error while sending: {exc!r}') from exc

    async def send_and_wait(self, topic: str, key: Optional[bytes],
                            value: Optional[bytes],
                            partition: Optional[int],
                            timestamp: Optional[float],
                            headers: Optional[HeadersArg],
                            *,
                            transactional_id: str = None) -> RecordMetadata:
        """Send message and wait for it to be delivered to broker(s)."""
        fut = await self.send(
            topic, key, value, partition, timestamp, headers,
        )
        return await fut

    async def flush(self) -> None:
        """Flush producer buffer.

        This will wait until the producer has written
        all buffered up messages to any connected brokers.
        """
        await self._producer_thread.flush()

    def key_partition(self, topic: str, key: bytes) -> TP:
        """Return topic and partition destination for key."""
        raise NotImplementedError()


class Transport(base.Transport):
    """Kafka transport using :pypi:`confluent_kafka`."""

    Consumer: ClassVar[Type[ConsumerT]] = Consumer
    Producer: ClassVar[Type[ProducerT]] = Producer

    default_port = 9092
    driver_version = f'confluent_kafka={confluent_kafka.__version__}'

    def _topic_config(self,
                      retention: int = None,
                      compacting: bool = None,
                      deleting: bool = None) -> MutableMapping[str, Any]:
        pass
