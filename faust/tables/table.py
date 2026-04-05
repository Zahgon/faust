"""Table (key/value changelog stream)."""
from typing import Any, ClassVar, Type

from mode import Seconds

from faust import windows
from faust.types.tables import KT, TableT, VT, WindowWrapperT
from faust.types.windows import WindowT
from faust.utils.terminal.tables import dict_as_ansitable

from . import wrappers
from .base import Collection

__all__ = ['Table']


class Table(TableT[KT, VT], Collection):
    """Table (non-windowed)."""

    WindowWrapper: ClassVar[Type[WindowWrapperT]] = wrappers.WindowWrapper

    def using_window(self, window: WindowT, *,
                     key_index: bool = False) -> WindowWrapperT:
        """Wrap table using a specific window type."""
        self.window = window
        self._changelog_compacting = True
        self._changelog_deleting = True
        self._changelog_topic = None  # will reset on next property access
        return self.WindowWrapper(self, key_index=key_index)

    def hopping(self, size: Seconds, step: Seconds,
                expires: Seconds = None,
                key_index: bool = False) -> WindowWrapperT:
        """Wrap table in a hopping window."""
        return self.using_window(
            windows.HoppingWindow(size, step, expires),
            key_index=key_index,
        )

    def tumbling(self, size: Seconds,
                 expires: Seconds = None,
                 key_index: bool = False) -> WindowWrapperT:
        """Wrap table in a tumbling window."""
        return self.using_window(
            windows.TumblingWindow(size, expires),
            key_index=key_index,
        )

    def __missing__(self, key: KT) -> VT:
        if self.default is not None:
            return self.default()
        raise KeyError(key)

    def _has_key(self, key: KT) -> bool:
        pass

    def _get_key(self, key: KT) -> VT:
        return self[key]

    def _set_key(self, key: KT, value: VT) -> None:
        pass

    def _del_key(self, key: KT) -> None:
        pass

    def on_key_get(self, key: KT) -> None:
        """Call when the value for a key in this table is retrieved."""
        pass

    def on_key_set(self, key: KT, value: VT) -> None:
        """Call when the value for a key in this table is set."""
        pass

    def on_key_del(self, key: KT) -> None:
        """Call when a key in this table is removed."""
        pass

    def as_ansitable(self, title: str = '{table.name}',
                     **kwargs: Any) -> str:
        """Draw table as a a terminal ANSI table."""
        pass
