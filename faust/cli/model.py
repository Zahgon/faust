"""Program ``faust model`` used to list details about a model."""
from datetime import datetime
from typing import Any, Sequence, Type

import click
from mode.utils import text

from faust.models import registry
from faust.types import FieldDescriptorT, ModelT
from faust.utils import terminal

from .base import AppCommand, argument

__all__ = ['model']

#: Built-in types show as name only (e.g. str).
#: Other types will use full `repr()`.
BUILTIN_TYPES = frozenset({int, float, str, bytes, datetime})


class model(AppCommand):
    """Show model detail."""

    headers = ['field', 'type', 'default']

    options = [
        argument('name'),
    ]

    async def run(self, name: str) -> None:
        """Dump list of registered models to terminal."""
        pass

    def _unknown_model(self, name: str, *,
                       lookup: str = None) -> click.UsageError:
        pass

    def model_fields(self, model: Type[ModelT]) -> terminal.TableDataT:
        """Convert model fields to terminal table rows."""
        pass

    def field(self, field: FieldDescriptorT) -> Sequence[str]:
        """Convert model field model to terminal table columns."""
        pass

    def _type(self, typ: Any) -> str:
        pass

    def model_to_row(self, model: Type[ModelT]) -> Sequence[str]:
        """Convert model to terminal table row."""
        pass

    def _name(self, model: Type[ModelT]) -> str:
        pass

    def _help(self, model: Type[ModelT]) -> str:
        pass
