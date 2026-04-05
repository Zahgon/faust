"""LiveCheck - Test runner."""
import asyncio
import logging
import traceback
import typing

from time import monotonic
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
)

from mode.utils.logging import CompositeLogger
from mode.utils.times import humanize_seconds
from mode.utils.typing import NoReturn

from faust.models import maybe_model

from .exceptions import (
    LiveCheckError,
    TestFailed,
    TestRaised,
    TestSkipped,
    TestTimeout,
)
from .locals import current_test_stack
from .models import State, TestExecution, TestReport
from .signals import BaseSignal

if typing.TYPE_CHECKING:
    from .case import Case as _Case
else:
    class _Case: ...  # noqa

__all__ = ['TestRunner']


class TestRunner:
    """Execute and keep track of test instance."""

    case: _Case

    state: State = State.INIT
    test: TestExecution

    started: float
    ended: Optional[float]
    runtime: Optional[float]

    logs: List[Tuple[str, Tuple]]
    signal_latency: Dict[str, float]

    report: Optional[TestReport] = None
    error: Optional[BaseException] = None

    def __init__(self,
                 case: _Case,
                 test: TestExecution,
                 started: float) -> None:
        self.case = case
        self.test = test
        self.started = started
        self.ended = None
        self.runtime = None
        self.logs = []
        self.log = CompositeLogger(
            self.case.log.logger,
            formatter=self._format_log,
        )
        self.signal_latency = {}

    async def execute(self) -> None:
        """Execute this test."""
        pass

    async def skip(self, reason: str) -> NoReturn:
        """Skip this test execution."""
        pass

    def _prepare_args(self, args: Iterable) -> Tuple:
        pass

    def _prepare_kwargs(self, kwargs: Mapping[str, Any]) -> Mapping[str, Any]:
        pass

    def _prepare_val(self, arg: Any) -> Any:
        pass

    def _format_log(self, severity: int, msg: str,
                    *args: Any, **kwargs: Any) -> str:
        pass

    async def on_skipped(self, exc: TestSkipped) -> None:
        """Call when a test execution was skipped."""
        pass

    async def on_start(self) -> None:
        """Call when a test starts executing."""
        pass

    async def on_signal_wait(self, signal: BaseSignal, timeout: float) -> None:
        """Call when the test is waiting for a signal."""
        self.log_info('∆ %r/%r %s (%rs)...',
                      signal.index,
                      self.case.total_signals,
                      signal.name.upper(),
                      timeout)

    async def on_signal_received(self,
                                 signal: BaseSignal,
                                 time_start: float,
                                 time_end: float) -> None:
        """Call when a signal related to this test is received."""
        latency = time_end - time_start
        self.signal_latency[signal.name] = latency

    async def on_failed(self, exc: BaseException) -> None:
        """Call when an invariant in the test has failed."""
        pass

    async def on_error(self, exc: BaseException) -> None:
        """Call when test execution raises error."""
        pass

    async def on_timeout(self, exc: BaseException) -> None:
        """Call when test execution times out."""
        pass

    async def on_pass(self) -> None:
        """Call when test execution returns successfully."""
        pass

    async def _finalize_report(self) -> None:
        pass

    def log_info(self, msg: str, *args: Any) -> None:
        """Log information related to the current execution."""
        if self.case.realtime_logs:
            self.log.info(msg, *args)
        else:
            self.logs.append((msg, args))

    def end(self) -> None:
        """End test execution."""
        pass

    def _flush_logs(self, severity: int = logging.INFO) -> None:
        pass
