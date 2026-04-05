"""Partition assignor."""
import socket
import zlib

from collections import defaultdict
from typing import (
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    Set,
    cast,
)

from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberAssignment,
    ConsumerProtocolMemberMetadata,
)
from mode import get_logger
from yarl import URL

from faust.types.app import AppT
from faust.types.assignor import (
    HostToPartitionMap,
    PartitionAssignorT,
    TopicToPartitionMap,
)
from faust.types.tables import TableManagerT
from faust.types.tuples import TP

from .client_assignment import ClientAssignment, ClientMetadata
from .cluster_assignment import ClusterAssignment
from .copartitioned_assignor import CopartitionedAssignor

__all__ = [
    'MemberAssignmentMapping',
    'MemberMetadataMapping',
    'MemberSubscriptionMapping',
    'ClientMetadataMapping',
    'ClientAssignmentMapping',
    'CopartitionedGroups',
    'PartitionAssignor',
]

MemberAssignmentMapping = MutableMapping[str, ConsumerProtocolMemberAssignment]
MemberMetadataMapping = MutableMapping[str, ConsumerProtocolMemberMetadata]
MemberSubscriptionMapping = MutableMapping[str, List[str]]
ClientMetadataMapping = MutableMapping[str, ClientMetadata]
ClientAssignmentMapping = MutableMapping[str, ClientAssignment]
CopartitionedGroups = MutableMapping[int, Iterable[Set[str]]]

logger = get_logger(__name__)


class PartitionAssignor(
        AbstractPartitionAssignor, PartitionAssignorT):  # type: ignore
    """PartitionAssignor handles internal topic creation.

    Further, this assignor needs to be sticky and potentially redundant

    Notes:
        Interface copied from :mod:`kafka.coordinator.assignors.abstract`.
    """

    _assignment: ClientAssignment
    _table_manager: TableManagerT
    _member_urls: MutableMapping[str, str]
    _changelog_distribution: HostToPartitionMap
    _active_tps: Set[TP]
    _standby_tps: Set[TP]
    _tps_url: MutableMapping[TP, str]
    _topic_groups: MutableMapping[str, int]

    def __init__(self, app: AppT, replicas: int = 0) -> None:
        AbstractPartitionAssignor.__init__(self)
        self.app = app
        self._table_manager = self.app.tables
        self._assignment = ClientAssignment(actives={}, standbys={})
        self._changelog_distribution = {}
        self.replicas = replicas
        self._member_urls = {}
        self._tps_url = {}
        self._active_tps = set()
        self._standby_tps = set()
        self._topic_groups = {}

    def group_for_topic(self, topic: str) -> int:
        return self._topic_groups[topic]

    @property
    def changelog_distribution(self) -> HostToPartitionMap:
        pass

    @changelog_distribution.setter
    def changelog_distribution(self, value: HostToPartitionMap) -> None:
        pass

    @property
    def _metadata(self) -> ClientMetadata:
        pass

    @property
    def _url(self) -> URL:
        pass

    def on_assignment(
            self, assignment: ConsumerProtocolMemberMetadata) -> None:
        pass

    def metadata(self, topics: Set[str]) -> ConsumerProtocolMemberMetadata:
        pass

    @classmethod
    def _group_co_subscribed(cls, topics: Set[str],
                             subscriptions: MemberSubscriptionMapping,
                             ) -> Iterable[Set[str]]:
        pass

    @classmethod
    def _get_copartitioned_groups(
            cls, topics: Set[str],
            cluster: ClusterMetadata,
            subscriptions: MemberSubscriptionMapping) -> CopartitionedGroups:
        pass

    @classmethod
    def _get_client_metadata(
            cls, metadata: ConsumerProtocolMemberMetadata) -> ClientMetadata:
        pass

    def _update_member_urls(self,
                            clients_metadata: ClientMetadataMapping) -> None:
        pass

    def assign(
            self,
            cluster: ClusterMetadata,
            member_metadata: MemberMetadataMapping) -> MemberAssignmentMapping:
        pass

    def _trace_assign(
            self,
            cluster: ClusterMetadata,
            member_metadata: MemberMetadataMapping) -> MemberAssignmentMapping:
        pass

    def _assign(
            self,
            cluster: ClusterMetadata,
            member_metadata: MemberMetadataMapping) -> MemberAssignmentMapping:
        pass

    def _perform_assignment(
            self,
            cluster: ClusterMetadata,
            member_metadata: MemberMetadataMapping) -> MemberAssignmentMapping:
        pass

    def _global_table_standby_assignments(
            self,
            assignments: ClientAssignmentMapping,
            partitions_by_topic: Mapping[str, int]) -> ClientAssignmentMapping:
        # Ensures all members have access to all changelog partitions
        # as standbys, if not already as actives
        pass

    def _protocol_assignments(
            self,
            assignments: ClientAssignmentMapping,
            cl_distribution: HostToPartitionMap,
            topic_groups: Mapping[str, int]) -> MemberAssignmentMapping:
        pass

    @classmethod
    def _compress(cls, raw: bytes) -> bytes:
        pass

    @classmethod
    def _decompress(cls, compressed: bytes) -> bytes:
        pass

    @classmethod
    def _topics_filtered(cls, assignment: TopicToPartitionMap,
                         topics: Set[str]) -> TopicToPartitionMap:
        return {
            topic: partitions
            for topic, partitions in assignment.items() if topic in topics
        }

    def _get_changelog_distribution(
            self, assignments: ClientAssignmentMapping) -> HostToPartitionMap:
        pass

    @property
    def name(self) -> str:
        pass

    @property
    def version(self) -> int:
        pass

    def assigned_standbys(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.standbys.items()
            for partition in partitions
        }

    def assigned_actives(self) -> Set[TP]:
        return {
            TP(topic, partition)
            for topic, partitions in self._assignment.actives.items()
            for partition in partitions
        }

    def table_metadata(self, topic: str) -> HostToPartitionMap:
        return {
            host: self._topics_filtered(assignment, {topic})
            for host, assignment in self.changelog_distribution.items()
        }

    def tables_metadata(self) -> HostToPartitionMap:
        return self.changelog_distribution

    def key_store(self, topic: str, key: bytes) -> URL:
        return URL(self._tps_url[self.app.producer.key_partition(topic, key)])

    def is_active(self, tp: TP) -> bool:
        pass

    def is_standby(self, tp: TP) -> bool:
        return tp in self._standby_tps
