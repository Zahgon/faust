"""LiveCheck - Models."""
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional

from mode.utils.compat import want_str
from mode.utils.objects import cached_property
from mode.utils.text import abbr

from faust import Record
from faust.utils.iso8601 import parse as parse_iso8601

__all__ = ['State', 'SignalEvent', 'TestExecution', 'TestReport']

HEADER_TEST_ID = 'LiveCheck-Test-Id'
HEADER_TEST_NAME = 'LiveCheck-Test-Name'
HEADER_TEST_TIMESTAMP = 'LiveCheck-Test-Timestamp'
HEADER_TEST_EXPIRES = 'LiveCheck-Test-Expires'


class State(Enum):
    """Test execution status."""

    INIT = 'INIT'
    PASS = 'PASS'
    FAIL = 'FAIL'
    ERROR = 'ERROR'
    TIMEOUT = 'TIMEOUT'
    STALL = 'STALL'
    SKIP = 'SKIP'

    def is_ok(self) -> bool:
        """Return :const:`True` if this is considered an OK state."""
        pass


OK_STATES = frozenset({State.INIT, State.PASS, State.SKIP})


class SignalEvent(Record):
    """Signal sent to test (see :class:`faust.livecheck.signals.Signal`)."""

    signal_name: str
    case_name: str
    key: Any
    value: Any


class TestExecution(Record, isodates=True):
    """Requested test execution."""

    id: str
    case_name: str
    timestamp: datetime
    test_args: List[Any]
    test_kwargs: Dict[str, Any]
    expires: datetime

    @classmethod
    def from_headers(cls, headers: Mapping) -> Optional['TestExecution']:
        """Create instance from mapping of HTTP/Kafka headers."""
        pass

    def as_headers(self) -> Mapping:
        """Return test metadata as mapping of HTTP/Kafka headers."""
        return {
            HEADER_TEST_ID: self.id,
            HEADER_TEST_NAME: self.case_name,
            HEADER_TEST_TIMESTAMP: self.timestamp.isoformat(),
            HEADER_TEST_EXPIRES: self.expires.isoformat(),
        }

    @cached_property
    def ident(self) -> str:
        """Return long identifier for this test used in logs."""
        pass

    @cached_property
    def shortident(self) -> str:
        """Return short identifier for this test used in logs."""
        pass

    def _build_ident(self, case_name: str, id: str) -> str:
        pass

    def _now(self) -> datetime:
        return datetime.utcnow().astimezone(timezone.utc)

    @cached_property
    def human_date(self) -> str:
        """Return human-readable description of test timestamp."""
        pass

    @cached_property
    def was_issued_today(self) -> bool:
        """Return :const:`True` if test was issued on todays date."""
        pass

    @cached_property
    def is_expired(self) -> bool:
        """Return :const:`True` if this test already expired."""
        pass

    @cached_property
    def short_case_name(self) -> str:
        """Return abbreviated case name."""
        pass


class TestReport(Record):
    """Report after test execution."""

    case_name: str
    state: State
    test: Optional[TestExecution]
    runtime: Optional[float]
    signal_latency: Dict[str, float]
    error: Optional[str]
    traceback: Optional[str]
