"""In-memory table storage."""
from typing import (
    Any,
    Callable,
    Iterable,
    MutableMapping,
    Optional,
    Set,
    Tuple,
)
from faust.types import EventT, TP
from . import base


class Store(base.Store):
    """Table storage using an in-memory dictionary."""

    def __post_init__(self) -> None:
        self.data: MutableMapping = {}

    def _clear(self) -> None:
        self.data.clear()

    def apply_changelog_batch(self, batch: Iterable[EventT],
                              to_key: Callable[[Any], Any],
                              to_value: Callable[[Any], Any]) -> None:
        """Apply batch of changelog events to in-memory table."""
        pass

    def _create_batch_iterator(
            self,
            mark_as_delete: Callable[[Any], None],
            to_key: Callable[[Any], Any],
            to_value: Callable[[Any], Any],
            batch: Iterable[EventT]) -> Iterable[Tuple[Any, Any]]:
        pass

    def persisted_offset(self, tp: TP) -> Optional[int]:
        """Return the persisted offset.

        This always returns :const:`None` when using the in-memory store.
        """
        return None

    def reset_state(self) -> None:
        """Remove local file system state.

        This does nothing when using the in-memory store.

        """
        ...
