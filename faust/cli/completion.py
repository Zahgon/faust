"""completion - Command line utility for completion.

Supports ``bash``, ``ksh``, ``zsh``, etc.
"""
import os
from pathlib import Path
from .base import AppCommand

try:
    import click_completion
except ImportError:  # pragma: no cover
    click_completion = None  # noqa
else:  # pragma: no cover
    click_completion.init()


class completion(AppCommand):
    """Output shell completion to be evaluated by the shell."""

    require_app = False

    async def run(self) -> None:
        """Dump click completion script for Faust CLI."""
        pass

    def shell(self) -> str:
        """Return the current shell used in this environment."""
        pass
