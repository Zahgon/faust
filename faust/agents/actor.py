"""Actor - Individual Agent instances."""
from typing import Any, AsyncGenerator, AsyncIterator, Coroutine, Set, cast
from mode import Service
from mode.utils.tracebacks import format_agen_stack, format_coro_stack

from faust.types import ChannelT, StreamT, TP
from faust.types.agents import (
    ActorT,
    AgentT,
    AsyncIterableActorT,
    AwaitableActorT,
    _T,
)
__all__ = ['Actor', 'AsyncIterableActor', 'AwaitableActor']


class Actor(ActorT, Service):
    """An actor is a specific agent instance."""

    mundane_level = 'debug'

    # Agent will start n * concurrency actors.

    def __init__(self,
                 agent: AgentT,
                 stream: StreamT,
                 it: _T,
                 index: int = None,
                 active_partitions: Set[TP] = None,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        self.index = index
        self.active_partitions = active_partitions
        self.actor_task = None
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        """Call when actor is starting."""
        pass

    async def on_stop(self) -> None:
        """Call when actor is being stopped."""
        pass

    async def on_isolated_partition_revoked(self, tp: TP) -> None:
        """Call when an isolated partition is being revoked."""
        pass

    async def on_isolated_partition_assigned(self, tp: TP) -> None:
        """Call when an isolated partition is being assigned."""
        pass

    def cancel(self) -> None:
        """Tell actor to stop reading from the stream."""
        cast(ChannelT, self.stream.channel)._throw(StopAsyncIteration())

    def __repr__(self) -> str:
        return f'<{self.shortlabel}>'

    @property
    def label(self) -> str:
        """Return human readable description of actor."""
        s = self.agent._agent_label(name_suffix='*')
        if self.stream.active_partitions:
            partitions = {
                tp.partition for tp in self.stream.active_partitions
            }
            s += f' isolated={partitions}'
        return s


class AsyncIterableActor(AsyncIterableActorT, Actor):
    """Used for agent function that yields."""

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()

    def traceback(self) -> str:
        pass


class AwaitableActor(AwaitableActorT, Actor):
    """Used for actor function that do not yield."""

    def __await__(self) -> Any:
        return self.it.__await__()

    def traceback(self) -> str:
        pass
