"""Monitor using Statsd."""
import typing

from typing import Any, Dict, Optional, cast

from mode.utils.objects import cached_property

from faust import web
from faust.exceptions import ImproperlyConfigured
from faust.types import (
    AppT,
    CollectionT,
    EventT,
    Message,
    PendingMessage,
    RecordMetadata,
    StreamT,
    TP,
)
from faust.types.assignor import PartitionAssignorT
from faust.types.transports import ConsumerT, ProducerT

from .monitor import Monitor, TPOffsetMapping

try:
    import statsd
except ImportError:  # pragma: no cover
    statsd = None

if typing.TYPE_CHECKING:  # pragma: no cover
    from statsd import StatsClient
else:
    class StatsClient: ...  # noqa

__all__ = ['StatsdMonitor']


class StatsdMonitor(Monitor):
    """Statsd Faust Sensor.

    This sensor, records statistics to Statsd along with computing metrics
    for the stats server
    """

    host: str
    port: int
    prefix: str

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 8125,
                 prefix: str = 'faust-app',
                 rate: float = 1.0,
                 **kwargs: Any) -> None:
        self.host = host
        self.port = port
        self.prefix = prefix
        self.rate = rate
        if statsd is None:
            raise ImproperlyConfigured(
                'StatsMonitor requires `pip install statsd`.')
        super().__init__(**kwargs)

    def _new_statsd_client(self) -> StatsClient:
        pass

    def on_message_in(self, tp: TP, offset: int, message: Message) -> None:
        """Call before message is delegated to streams."""
        pass

    def on_stream_event_in(self, tp: TP, offset: int, stream: StreamT,
                           event: EventT) -> Optional[Dict]:
        """Call when stream starts processing an event."""
        pass

    def _stream_label(self, stream: StreamT) -> str:
        pass

    def on_stream_event_out(self, tp: TP, offset: int, stream: StreamT,
                            event: EventT, state: Dict = None) -> None:
        """Call when stream is done processing an event."""
        pass

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

    def on_commit_completed(self, consumer: ConsumerT, state: Any) -> None:
        """Call when consumer commit offset operation completed."""
        pass

    def on_send_initiated(self, producer: ProducerT, topic: str,
                          message: PendingMessage,
                          keysize: int, valsize: int) -> Any:
        """Call when message added to producer buffer."""
        self.client.incr(f'topic.{topic}.messages_sent', rate=self.rate)
        return super().on_send_initiated(
            producer, topic, message, keysize, valsize)

    def on_send_completed(self,
                          producer: ProducerT,
                          state: Any,
                          metadata: RecordMetadata) -> None:
        """Call when producer finished sending message."""
        super().on_send_completed(producer, state, metadata)
        self.client.incr('messages_sent', rate=self.rate)
        self.client.timing(
            'send_latency',
            self.ms_since(cast(float, state)),
            rate=self.rate)

    def on_send_error(self,
                      producer: ProducerT,
                      exc: BaseException,
                      state: Any) -> None:
        """Call when producer was unable to publish message."""
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
        super().on_rebalance_return(app, state)
        self.client.decr('rebalances', rate=self.rate)
        self.client.incr('rebalances_recovering', rate=self.rate)
        self.client.timing(
            'rebalance_return_latency',
            self.ms_since(state['time_return']),
            rate=self.rate)

    def on_rebalance_end(self, app: AppT, state: Dict) -> None:
        """Cluster rebalance fully completed (including recovery)."""
        pass

    def count(self, metric_name: str, count: int = 1) -> None:
        """Count metric by name."""
        super().count(metric_name, count=count)
        self.client.incr(metric_name, count=count, rate=self.rate)

    def on_tp_commit(self, tp_offsets: TPOffsetMapping) -> None:
        """Call when offset in topic partition is committed."""
        pass

    def track_tp_end_offset(self, tp: TP, offset: int) -> None:
        """Track new topic partition end offset for monitoring lags."""
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

    @cached_property
    def client(self) -> StatsClient:
        """Return statsd client."""
        pass
