"""LiveCheck - Test cases."""
import traceback
import typing

from collections import deque
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from itertools import count
from random import uniform
from statistics import median
from time import monotonic
from typing import (
    Any,
    ClassVar,
    Dict,
    Iterable,
    Optional,
    Type,
    Union,
    cast,
)

from aiohttp import ClientError, ClientTimeout
from mode import Seconds, Service, want_seconds
from mode.utils.contexts import asynccontextmanager
from mode.utils.times import humanize_seconds
from mode.utils.typing import AsyncGenerator, Counter, Deque
from yarl import URL

from faust.utils import uuid
from faust.utils.functional import deque_pushpopmax

from .exceptions import ServiceDown, SuiteFailed, SuiteStalled
from .locals import current_execution_stack, current_test_stack
from .models import SignalEvent, State, TestExecution, TestReport
from .runners import TestRunner
from .signals import BaseSignal

if typing.TYPE_CHECKING:
    from .app import LiveCheck as _LiveCheck
else:
    class _LiveCheck: ...  # noqa

__all__ = ['Case']


class Case(Service):
    """LiveCheck test case."""

    Runner: ClassVar[Type[TestRunner]] = TestRunner

    app: _LiveCheck

    #: Name of the test
    #: If not set this will be generated out of the subclass name.
    name: str

    active: bool = True

    #: Current state of this test.
    status: State = State.INIT

    #: How often we execute the test using fake data
    #: (define Case.make_fake_request()).
    #:
    #: Set to None if production traffic is frequent enough to
    #: satisfy :attr:`warn_stalled_after`.
    frequency: Optional[float] = None

    #: Timeout in seconds for when after we warn that nothing is processing.
    warn_stalled_after: float = 1800.0

    #: Probability of test running when live traffic is going through.
    probability: float = 0.5

    max_consecutive_failures: int = 30

    #: The warn_stalled_after timer uses this to keep track of
    #: either when a test was last received, or the last time the timer
    #: timed out.
    last_test_received: Optional[float] = None

    #: Timestamp of when the suite last failed.
    last_fail: Optional[float] = None

    #: Max items to store in :attr:`latency_history` and
    #: :attr:`runtime_history`.
    max_history: int = 100
    latency_history: Deque[float]
    frequency_history: Deque[float]
    runtime_history: Deque[float]

    runtime_avg: Optional[float] = None
    latency_avg: Optional[float] = None
    frequency_avg: Optional[float] = None

    signals: Dict[str, BaseSignal]
    _original_signals: Iterable[BaseSignal]

    total_signals: int
    test_expires: timedelta = timedelta(hours=3)

    realtime_logs = False

    url_timeout_total: Optional[float] = 5 * 60.0
    url_timeout_connect: Optional[float] = None
    url_error_retries: int = 10
    url_error_delay_min: float = 0.5
    url_error_delay_backoff: float = 1.5
    url_error_delay_max: float = 5.0

    state_transition_delay: float = 60.0

    consecutive_failures: int = 0
    total_failures: int = 0
    total_by_state: Counter[State]

    def __init__(self, *,
                 app: _LiveCheck,
                 name: str,
                 probability: float = None,
                 warn_stalled_after: Seconds = None,
                 active: bool = None,
                 signals: Iterable[BaseSignal] = None,
                 test_expires: Seconds = None,
                 frequency: Seconds = None,
                 realtime_logs: bool = None,
                 max_history: int = None,
                 max_consecutive_failures: int = None,
                 url_timeout_total: float = None,
                 url_timeout_connect: float = None,
                 url_error_retries: int = None,
                 url_error_delay_min: float = None,
                 url_error_delay_backoff: float = None,
                 url_error_delay_max: float = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.name = name
        if active is not None:
            self.active = active
        if probability is not None:
            self.probability = probability
        if warn_stalled_after is not None:
            self.warn_stalled_after = want_seconds(warn_stalled_after)
        self._original_signals = signals or ()
        self.signals = {
            sig.name: sig.clone(case=self)
            for sig in self._original_signals
        }
        if test_expires is not None:
            self.test_expires = timedelta(seconds=want_seconds(test_expires))
        if frequency is not None:
            self.frequency = want_seconds(frequency)
        if realtime_logs is not None:
            self.realtime_logs = realtime_logs
        if max_history is not None:
            self.max_history = max_history
        if max_consecutive_failures is not None:
            self.max_consecutive_failures = max_consecutive_failures

        if url_timeout_total is not None:
            self.url_timeout_total = url_timeout_total
        if url_timeout_connect is not None:
            self.url_timeout_connect = url_timeout_connect
        if url_error_retries is not None:
            self.url_error_retries = url_error_retries
        if url_error_delay_min is not None:
            self.url_error_delay_min = url_error_delay_min
        if url_error_delay_backoff is not None:
            self.url_error_delay_backoff = url_error_delay_backoff
        if url_error_delay_max is not None:
            self.url_error_delay_max = url_error_delay_max

        self.frequency_history = deque()
        self.latency_history = deque()
        self.runtime_history = deque()

        self.total_by_state = Counter()

        self.total_signals = len(self.signals)
        # update local attribute so that the
        # signal attributes have the correct signal instance.
        self.__dict__.update(self.signals)

        Service.__init__(self, **kwargs)

    @Service.timer(10.0)
    async def _sampler(self) -> None:
        pass

    async def _sample(self) -> None:
        pass

    @asynccontextmanager
    async def maybe_trigger(
            self, id: str = None,
            *args: Any,
            **kwargs: Any) -> AsyncGenerator[Optional[TestExecution], None]:
        """Schedule test execution, or not, based on probability setting."""
        execution: Optional[TestExecution] = None
        with ExitStack() as exit_stack:
            if uniform(0, 1) < self.probability:
                execution = await self.trigger(id, *args, **kwargs)
                exit_stack.enter_context(current_test_stack.push(execution))
            yield execution

    async def trigger(self, id: str = None,
                      *args: Any,
                      **kwargs: Any) -> TestExecution:
        """Schedule test execution ASAP."""
        id = id or uuid()
        execution = TestExecution(
            id=id,
            case_name=self.name,
            timestamp=self._now(),
            test_args=args,
            test_kwargs=kwargs,
            expires=self._now() + self.test_expires,
        )
        await self.app.pending_tests.send(key=id, value=execution)
        return execution

    def _now(self) -> datetime:
        return datetime.utcnow().astimezone(timezone.utc)

    async def run(self, *test_args: Any, **test_kwargs: Any) -> None:
        """Override this to define your test case."""
        raise NotImplementedError('Case class must implement run')

    async def resolve_signal(self, key: str, event: SignalEvent) -> None:
        """Mark test execution signal as resolved."""
        pass

    async def execute(self, test: TestExecution) -> None:
        """Execute test using :class:`TestRunner`."""
        pass

    async def on_test_start(self, runner: TestRunner) -> None:
        """Call when a test starts executing."""
        pass

    async def on_test_skipped(self, runner: TestRunner) -> None:
        """Call when a test is skipped."""
        pass

    async def on_test_failed(self,
                             runner: TestRunner,
                             exc: BaseException) -> None:
        """Call when invariant in test execution fails."""
        pass

    async def on_test_error(self,
                            runner: TestRunner,
                            exc: BaseException) -> None:
        """Call when a test execution raises an exception."""
        pass

    async def on_test_timeout(self,
                              runner: TestRunner,
                              exc: BaseException) -> None:
        """Call when a test execution times out."""
        pass

    async def _set_test_error_state(self, state: State) -> None:
        pass

    def _set_pass_state(self, state: State) -> None:
        pass

    async def on_test_pass(self, runner: TestRunner) -> None:
        """Call when a test execution passes."""
        pass

    async def post_report(self, report: TestReport) -> None:
        """Publish test report."""
        pass

    @Service.task
    async def _send_frequency(self) -> None:
        pass

    async def make_fake_request(self) -> None:
        ...

    @Service.task
    async def _check_frequency(self) -> None:
        pass

    async def on_suite_fail(self,
                            exc: SuiteFailed,
                            new_state: State = State.FAIL) -> None:
        """Call when the suite fails."""
        pass

    def _maybe_recover_from_failed_state(self) -> None:
        pass

    def _failed_longer_than(self, secs: float) -> bool:
        pass

    @property
    def seconds_since_last_fail(self) -> Optional[float]:
        """Return number of seconds since any test failed."""
        pass

    async def get_url(self, url: Union[str, URL],
                      **kwargs: Any) -> Optional[bytes]:
        """Perform GET request using HTTP client."""
        pass

    async def post_url(self, url: Union[str, URL],
                       **kwargs: Any) -> Optional[bytes]:
        """Perform POST request using HTTP client."""
        pass

    async def url_request(self, method: str, url: Union[str, URL],
                          **kwargs: Any) -> Optional[bytes]:
        """Perform URL request using HTTP client."""
        pass

    @property
    def current_test(self) -> Optional[TestExecution]:
        """Return the currently active test in this task (if any)."""
        pass

    @property
    def current_execution(self) -> Optional[TestRunner]:
        """Return the currently executing :class:`TestRunner` in this task."""
        pass

    @property
    def label(self) -> str:
        """Return human-readable label for this test case."""
        return f'{type(self).__name__}: {self.name}'
