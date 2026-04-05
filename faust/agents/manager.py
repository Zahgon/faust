"""Agent manager."""
import asyncio

from collections import defaultdict
from typing import Any, Dict, List, Mapping, MutableMapping, MutableSet, Set
from weakref import WeakSet

from mode import Service
from mode.utils.collections import ManagedUserDict
from mode.utils.compat import OrderedDict
from mode.utils.locks import Event

from faust.types import AgentManagerT, AgentT, AppT
from faust.types.tuples import TP, tp_set_to_map
from faust.utils.tracing import traced_from_parent_span

TRACEBACK_HEADER = '''
=======================================
 TRACEBACK OF ALL RUNNING AGENT ACTORS
=======================================
'''

TRACEBACK_FORMAT = '''
* {name} ----->
============================================================
{traceback}

'''

TRACEBACK_FOOTER = '''
-eof tracebacks- :-)
'''


class AgentManager(Service, AgentManagerT, ManagedUserDict):
    """Agent manager."""

    traceback_header: str = TRACEBACK_HEADER
    traceback_format: str = TRACEBACK_FORMAT
    traceback_footer: str = TRACEBACK_FOOTER

    _by_topic: MutableMapping[str, MutableSet[AgentT]]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.data = OrderedDict()
        self._by_topic = defaultdict(WeakSet)
        self._agents_started = Event()
        Service.__init__(self, **kwargs)

    def __hash__(self) -> int:
        return object.__hash__(self)

    async def on_start(self) -> None:
        """Call when agents are being started."""
        pass

    def actor_tracebacks(self) -> Mapping[str, List[str]]:
        pass

    def human_tracebacks(self) -> str:
        pass

    async def wait_until_agents_started(self) -> None:
        pass

    def service_reset(self) -> None:
        """Reset service state on restart."""
        pass

    async def on_stop(self) -> None:
        """Call when agents are being stopped."""
        pass

    async def stop(self) -> None:
        """Stop all running agents."""
        # Cancel first so _execute_actor sees we are not stopped.
        self.cancel()
        # Then stop the agents
        await super().stop()

    def cancel(self) -> None:
        """Cancel all running agents."""
        [agent.cancel() for agent in self.values()]

    def update_topic_index(self) -> None:
        """Update indices."""
        pass

    async def on_rebalance(self,
                           revoked: Set[TP],
                           newly_assigned: Set[TP]) -> None:
        """Call when a rebalance is needed."""
        T = traced_from_parent_span()
        # for isolated_partitions agents we stop agents for revoked
        # partitions.
        for agent, tps in self._collect_agents_for_update(revoked).items():
            await T(agent.on_partitions_revoked)(tps)
        # for isolated_partitions agents we start agents for newly
        # assigned partitions
        for agent, tps in T(self._collect_agents_for_update)(
                newly_assigned).items():
            await T(agent.on_partitions_assigned)(tps)

    def _collect_agents_for_update(
            self, tps: Set[TP]) -> Dict[AgentT, Set[TP]]:
        by_agent: Dict[AgentT, Set[TP]] = defaultdict(set)
        for topic, tps in tp_set_to_map(tps).items():
            for agent in self._by_topic[topic]:
                by_agent[agent].update(tps)
        return by_agent
