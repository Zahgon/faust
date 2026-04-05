"""Copartitioned Assignor."""
from itertools import cycle
from math import ceil
from typing import Iterable, Iterator, MutableMapping, Optional, Sequence, Set
from mode.utils.typing import Counter
from .client_assignment import CopartitionedAssignment

__all__ = ['CopartitionedAssignor']


class CopartitionedAssignor:
    """Copartitioned Assignor.

    All copartitioned topics must have the same number of partitions

    The assignment is sticky which uses the following heuristics:

    - Maintain existing assignments as long as within capacity for each client
    - Assign actives to standbys when possible (within capacity)
    - Assign in order to fill capacity of the clients

    We optimize for not over utilizing resources instead of under-utilizing
    resources. This results in a balanced assignment when capacity is the
    default value which is ``ceil(num partitions / num clients)``

    Notes:
        Currently we raise an exception if number of clients is not enough
        for the desired `replication`.
    """

    capacity: int
    num_partitions: int
    replicas: int
    topics: Set[str]

    _num_clients: int
    _client_assignments: MutableMapping[str, CopartitionedAssignment]

    def __init__(self,
                 topics: Iterable[str],
                 cluster_asgn: MutableMapping[str, CopartitionedAssignment],
                 num_partitions: int,
                 replicas: int,
                 capacity: int = None) -> None:
        self._num_clients = len(cluster_asgn)
        assert self._num_clients, 'Should assign to at least 1 client'
        self.num_partitions = num_partitions
        self.replicas = min(replicas, self._num_clients - 1)
        self.capacity = (
            int(ceil(float(self.num_partitions) / self._num_clients))
            if capacity is None else capacity
        )
        self.topics = set(topics)

        assert self.capacity * self._num_clients >= self.num_partitions, \
            'Not enough capacity'

        self._client_assignments = cluster_asgn

    def get_assignment(self) -> MutableMapping[str, CopartitionedAssignment]:
        pass

    def _all_assigned(self, active: bool) -> bool:
        pass

    def _assign(self, active: bool) -> None:
        pass

    def _assigned_partition_counts(self, active: bool) -> Counter[int]:
        pass

    def _get_client_limit(self, active: bool) -> int:
        pass

    def _total_assigns_per_partition(self, active: bool) -> int:
        pass

    def _unassign_overassigned(self, active: bool) -> None:
        # There are cases when multiple clients could have the same
        # assignment (zombies).  We need to handle that appropriately.
        pass

    def _get_unassigned(self, active: bool) -> Sequence[int]:
        pass

    def _can_assign(self, assignment: CopartitionedAssignment, partition: int,
                    active: bool) -> bool:
        pass

    def _client_exhausted(self, assignemnt: CopartitionedAssignment,
                          active: bool, client_limit: int = None) -> bool:
        pass

    def _find_promotable_standby(self, partition: int,
                                 candidates: Iterator[CopartitionedAssignment],
                                 ) -> Optional[CopartitionedAssignment]:
        # Round robin to find standby until we make a full cycle
        pass

    def _find_round_robin_assignable(self, partition: int,
                                     candidates: Iterator[
                                         CopartitionedAssignment],
                                     active: bool,
                                     ) -> Optional[CopartitionedAssignment]:
        # Round robin and assign until we make a full circle
        pass

    def _assign_round_robin(self, unassigned: Iterable[int],
                            active: bool) -> None:
        # We do round robin assignment as follows:
        # - For actives, we first try to assign to a standby
        # - For standby, we offset the start for round robin to evenly
        # distribute standbys for colocated actives
        # - We do round robin
        # - If no assignment found, it must be a standby and the only unfilled
        # client(s) must be actives/standbys for the partition
        # - If no assignment found, we unassign and arbitrary partition from a
        # filled assignment such that the partition can be assigned to it
        # - This guarantees eventual assignment of all partitions
        pass
