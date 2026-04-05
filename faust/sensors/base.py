"""Base-interface for sensors."""
from time import monotonic
from typing import Any, Dict, Iterator, Mapping, Optional, Set

from mode import Service

from faust import web
from faust.types import AppT, CollectionT, EventT, StreamT
from faust.types.assignor import PartitionAssignorT
from faust.types.tuples import Message, PendingMessage, RecordMetadata, TP
from faust.types.sensors import SensorDelegateT, SensorT
from faust.types.transports import ConsumerT, ProducerT

__all__ = ['Sensor', 'SensorDelegate']


class Sensor(SensorT, Service):
    """Base class for sensors.

    This sensor does not do anything at all, but can be subclassed
    to create new monitors.
    """

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Message received by a consumer."""
        ...

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> Optional[Dict]:
        """Message sent to a stream as an event."""
        pass

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: Dict = None) -> None:
        """Event was acknowledged by stream.

        Notes:
            Acknowledged means a stream finished processing the event, but
            given that multiple streams may be handling the same event,
            the message cannot be committed before all streams have
            processed it.  When all streams have acknowledged the event,
            it will go through :meth:`on_message_out` just before offsets
            are committed.
        """
        ...

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        """All streams finished processing message."""
        ...

    def on_topic_buffer_full(self, tp: TP) -> None:
        """Topic buffer full so conductor had to wait."""
        ...

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Key retrieved from table."""
        ...

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Value set for key in table."""
        ...

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Key deleted from table."""
        ...

    def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        """Consumer is about to commit topic offset."""
        ...

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        """Consumer finished committing topic offset."""
        ...

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> Any:
        """About to send a message."""
        ...

    def on_send_completed(self,
                          producer: ProducerT,
                          state: Any,
                          metadata: RecordMetadata) -> None:
        """Message successfully sent."""
        ...

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: Any) -> None:
        """Error while sending message."""
        ...

    def on_assignment_start(self,
                            assignor: PartitionAssignorT) -> Dict:
        """Partition assignor is starting to assign partitions."""
        pass

    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: Dict,
                            exc: BaseException) -> None:
        """Partition assignor did not complete assignor due to error."""
        ...

    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: Dict) -> None:
        """Partition assignor completed assignment."""
        ...

    def on_rebalance_start(self, app: AppT) -> Dict:
        """Cluster rebalance in progress."""
        pass

    def on_rebalance_return(self, app: AppT, state: Dict) -> None:
        """Consumer replied assignment is done to broker."""
        ...

    def on_rebalance_end(self, app: AppT, state: Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        ...

    def on_web_request_start(self, app: AppT, request: web.Request, *,
                             view: web.View = None) -> Dict:
        """Web server started working on request."""
        pass

    def on_web_request_end(self,
                           app: AppT,
                           request: web.Request,
                           response: Optional[web.Response],
                           state: Dict,
                           *,
                           view: web.View = None) -> None:
        """Web server finished working on request."""
        ...

    def asdict(self) -> Mapping:
        """Convert sensor state to dictionary."""
        return {}


class SensorDelegate(SensorDelegateT):
    """A class that delegates sensor methods to a list of sensors."""

    _sensors: Set[SensorT]

    def __init__(self, app: AppT) -> None:
        self.app = app
        self._sensors = set()

    def add(self, sensor: SensorT) -> None:
        """Add sensor."""
        # connect beacons
        sensor.beacon = self.app.beacon.new(sensor)
        self._sensors.add(sensor)

    def remove(self, sensor: SensorT) -> None:
        """Remove sensor."""
        pass

    def __iter__(self) -> Iterator:
        return iter(self._sensors)

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        pass

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> Optional[Dict]:
        """Call when stream starts processing an event."""
        pass

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: Dict = None) -> None:
        """Call when stream is done processing an event."""
        pass

    def on_topic_buffer_full(self, tp: TP) -> None:
        """Call when conductor topic buffer is full and has to wait."""
        for sensor in self._sensors:
            sensor.on_topic_buffer_full(tp)

    def on_message_out(self,
                       tp: TP,
                       offset: int,
                       message: Message) -> None:
        """Call when message is fully acknowledged and can be committed."""
        pass

    def on_table_get(self, table: CollectionT, key: Any) -> None:
        """Call when value in table is retrieved."""
        pass

    def on_table_set(self, table: CollectionT, key: Any, value: Any) -> None:
        """Call when new value for key in table is set."""
        pass

    def on_table_del(self, table: CollectionT, key: Any) -> None:
        """Call when key in a table is deleted."""
        pass

    def on_commit_initiated(self, consumer: ConsumerT) -> Any:
        """Call when consumer commit offset operation starts."""
        pass

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        """Call when consumer commit offset operation completed."""
        pass

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> Any:
        """Call when message added to producer buffer."""
        return {
            sensor: sensor.on_send_initiated(
                producer, topic, message, keysize, valsize)
            for sensor in self._sensors
        }

    def on_send_completed(self,
                          producer: ProducerT,
                          state: Any,
                          metadata: RecordMetadata) -> None:
        """Call when producer finished sending message."""
        for sensor in self._sensors:
            sensor.on_send_completed(producer, state[sensor], metadata)

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: Any) -> None:
        """Call when producer was unable to publish message."""
        pass

    def on_assignment_start(self,
                            assignor: PartitionAssignorT) -> Dict:
        """Partition assignor is starting to assign partitions."""
        pass

    def on_assignment_error(self,
                            assignor: PartitionAssignorT,
                            state: Dict,
                            exc: BaseException) -> None:
        """Partition assignor did not complete assignor due to error."""
        pass

    def on_assignment_completed(self,
                                assignor: PartitionAssignorT,
                                state: Dict) -> None:
        """Partition assignor completed assignment."""
        pass

    def on_rebalance_start(self, app: AppT) -> Dict:
        """Cluster rebalance in progress."""
        pass

    def on_rebalance_return(self, app: AppT, state: Dict) -> None:
        """Consumer replied assignment is done to broker."""
        for sensor in self._sensors:
            sensor.on_rebalance_return(app, state[sensor])

    def on_rebalance_end(self, app: AppT, state: Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        pass

    def on_web_request_start(self, app: AppT, request: web.Request, *,
                             view: web.View = None) -> Dict:
        """Web server started working on request."""
        pass

    def on_web_request_end(self,
                           app: AppT,
                           request: web.Request,
                           response: Optional[web.Response],
                           state: Dict,
                           *,
                           view: web.View = None) -> None:
        """Web server finished working on request."""
        pass

    def __repr__(self) -> str:
        return f'<{type(self).__name__}: {self._sensors!r}>'
