"""Program ``faust models`` used to list models available."""
from operator import attrgetter
from typing import Any, Callable, Sequence, Type, cast
from faust.models import registry
from faust.types import ModelT
from .base import AppCommand, option

__all__ = ['models']


class models(AppCommand):
    """List all available models as a tabulated list."""

    title = 'Models'
    headers = ['name', 'help']
    sortkey = attrgetter('_options.namespace')

    options = [
        option('--builtins/--no-builtins', default=False),
    ]

    async def run(self, *, builtins: bool) -> None:
        """Dump list of available models in this application."""
        pass

    def models(self, builtins: bool) -> Sequence[Type[ModelT]]:
        """Convert list of models to terminal table rows."""
        pass

    def model_to_row(self, model: Type[ModelT]) -> Sequence[str]:
        """Convert model fields to terminal table columns."""
        pass

    def _name(self, model: Type[ModelT]) -> str:
        pass

    def _help(self, model: Type[ModelT]) -> str:
        pass
