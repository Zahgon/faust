"""Client Assignment."""
import copy
from typing import List, Mapping, MutableMapping, Sequence, Set, Tuple, cast
from faust.models import Record
from faust.types import TP
from faust.types.assignor import HostToPartitionMap
from faust.types.tables import TableManagerT

R_COPART_ASSIGNMENT = '''
<{name} actives={self.actives} standbys={self.standbys} topics={self.topics}>
'''.strip()


class CopartitionedAssignment:
    """Copartitioned Assignment."""

    actives: Set[int]
    standbys: Set[int]
    topics: Set[str]

    def __init__(self,
                 actives: Set[int] = None,
                 standbys: Set[int] = None,
                 topics: Set[str] = None) -> None:
        self.actives = actives or set()
        self.standbys = standbys or set()
        self.topics = topics or set()

    def validate(self) -> None:
        pass

    def num_assigned(self, active: bool) -> int:
        pass

    def get_unassigned(self, num_partitions: int, active: bool) -> Set[int]:
        pass

    def pop_partition(self, active: bool) -> int:
        pass

    def unassign_partition(self, partition: int, active: bool) -> None:
        pass

    def assign_partition(self, partition: int, active: bool) -> None:
        pass

    def unassign_extras(self, capacity: int, replicas: int) -> None:
        pass

    def partition_assigned(self, partition: int, active: bool) -> bool:
        pass

    def promote_standby_to_active(self, standby_partition: int) -> None:
        pass

    def get_assigned_partitions(self, active: bool) -> Set[int]:
        pass

    def can_assign(self, partition: int, active: bool) -> bool:
        pass

    def __repr__(self) -> str:
        return R_COPART_ASSIGNMENT.format(
            name=type(self).__name__,
            self=self,
        )


class ClientAssignment(Record,
                       serializer='json',
                       include_metadata=False,
                       namespace='@ClientAssignment'):
    """Client Assignment data model."""

    actives: MutableMapping[str, List[int]]   # Topic -> Partition
    standbys: MutableMapping[str, List[int]]  # Topic -> Partition

    @property
    def active_tps(self) -> Set[TP]:
        pass

    @property
    def standby_tps(self) -> Set[TP]:
        pass

    def _get_tps(self, active: bool) -> Set[TP]:
        pass

    def kafka_protocol_assignment(
            self,
            table_manager: TableManagerT) -> Sequence[Tuple[str, List[int]]]:
        pass

    def add_copartitioned_assignment(
            self, assignment: CopartitionedAssignment) -> None:
        pass

    def copartitioned_assignment(
            self, topics: Set[str]) -> CopartitionedAssignment:
        pass

    def _colocated_partitions(
            self, topics: Set[str], active: bool) -> Set[int]:
        pass


class ClientMetadata(Record,
                     serializer='json',
                     include_metadata=False,
                     namespace='@ClientMetadata'):
    """Client Metadata data model."""

    assignment: ClientAssignment
    url: str
    changelog_distribution: HostToPartitionMap
    topic_groups: Mapping[str, int] = cast(Mapping[str, int], None)

    def __post_init__(self) -> None:
        if self.topic_groups is None:
            self.topic_groups = {}
