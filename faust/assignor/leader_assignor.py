"""Leader assignor."""
from typing import Any
from mode import Service
from mode.utils.objects import cached_property
from faust.types import AppT, TP, TopicT
from faust.types.assignor import LeaderAssignorT

__all__ = ['LeaderAssignor']


class LeaderAssignor(Service, LeaderAssignorT):
    """Leader assignor, ensures election of a leader."""

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        Service.__init__(self, **kwargs)
        self.app = app

    async def on_start(self) -> None:
        pass

    async def _enable_leader_topic(self) -> None:
        pass

    @cached_property
    def _leader_topic(self) -> TopicT:
        pass

    @cached_property
    def _leader_topic_name(self) -> str:
        pass

    @cached_property
    def _leader_tp(self) -> TP:
        pass

    def is_leader(self) -> bool:
        return self._leader_tp in self.app.consumer.assignment()
