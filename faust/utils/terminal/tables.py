"""Using :pypi:`terminaltables` to draw ANSI tables."""
import sys

from operator import itemgetter
from typing import (
    Any,
    Callable,
    IO,
    Iterable,
    List,
    Mapping,
    Sequence,
    Type,
    cast,
)

from mode.utils import logging
from mode.utils import text
from mode.utils.compat import isatty

from terminaltables import AsciiTable, SingleTable
from terminaltables.base_table import BaseTable as Table

__all__ = ['Table', 'TableDataT', 'table', 'logtable']

TableDataT = Sequence[Sequence[str]]


def table(data: TableDataT,
          *,
          title: str,
          target: IO = None,
          tty: bool = None,
          **kwargs: Any) -> Table:
    """Create suitable :pypi:`terminaltables` table for target.

    Arguments:
        data (Sequence[Sequence[str]]): Table data.

        target (IO): Target should be the destination output file
                     for your table, and defaults to :data:`sys.stdout`.
                     ANSI codes will be used if the target has a controlling
                     terminal, but not otherwise, which is why it's important
                     to pass the correct output file.
    """
    pass


def logtable(data: TableDataT,
             *,
             title: str,
             target: IO = None,
             tty: bool = None,
             headers: Sequence[str] = None,
             **kwargs: Any) -> str:
    """Prepare table for logging.

    Will use ANSI escape codes if the log file is a tty.
    """
    pass


def _get_best_table_type(tty: bool) -> Type[Table]:
    pass


def dict_as_ansitable(d: Mapping,
                      *,
                      key: str = 'Key',
                      value: str = 'Value',
                      sort: bool = False,
                      sortkey: Callable[[Any], Any] = itemgetter(0),
                      target: IO = sys.stdout,
                      title: str = None) -> str:
    pass
