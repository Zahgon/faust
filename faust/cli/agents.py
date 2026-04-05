"""Program ``faust agents`` used to list agents."""
from operator import attrgetter
from typing import Any, Callable, Optional, Sequence, Type, cast
from faust.types import AgentT
from .base import AppCommand, option


class agents(AppCommand):
    """List agents."""

    title = 'Agents'
    headers = ['name', 'topic', 'help']
    sortkey = attrgetter('name')

    options = [
        option(
            '--local/--no-local', help='Include agents using a local channel'),
    ]

    async def run(self, local: bool) -> None:
        """Dump list of available agents in this application."""
        pass

    def agents(self, *, local: bool = False) -> Sequence[AgentT]:
        """Convert list of agents to terminal table rows."""
        pass

    def agent_to_row(self, agent: AgentT) -> Sequence[str]:
        """Convert agent fields to terminal table row."""
        pass

    def _name(self, agent: AgentT) -> str:
        pass

    def _maybe_topic(self, agent: AgentT) -> Optional[str]:
        pass

    def _topic(self, agent: AgentT) -> str:
        pass

    def _help(self, agent: AgentT) -> str:
        pass
