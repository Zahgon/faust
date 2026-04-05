"""Program ``faust tables`` used to list tables."""
from .base import AppCommand

DEFAULT_TABLE_HELP = 'Missing description: use Table(.., help="str")'


class tables(AppCommand):
    """List available tables."""

    title = 'Tables'

    async def run(self) -> None:
        """Dump list of application tables to terminal."""
        pass
