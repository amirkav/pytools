import hashlib
import math
from typing import Dict, List, Optional, Set


class PartitionManagerError(Exception):
    """
    Main error for `PartitionManager`
    """


class Partition:
    def __init__(self, name: str, weight: int, seed: str) -> None:
        self.name = name
        self.weight = weight
        self.seed = seed

    def __str__(self) -> str:
        return f"Partition=[name={self.name}, seed={self.seed}, weight={self.weight}]"


class PartitionManager:
    # inspired by - Weighted Rendezvous Consistent Hashing
    # https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf

    DEFAULT_WEIGHT = 1

    def __init__(
        self,
        partition_names: Set[str],
        partition_weights: Optional[Dict[str, int]] = None,
        partition_seeds: Optional[Dict[str, str]] = None,
    ) -> None:
        self.partition_names = partition_names
        self.partition_weights = partition_weights if partition_weights is not None else dict()
        self.partition_seeds = partition_seeds if partition_seeds is not None else dict()
        self._partitions: Dict[str, Partition] = dict()
        self._generate_partitions()

    def _generate_partitions(self) -> None:
        if not self.partition_names:
            raise PartitionManagerError("partition_names cannot be empty")

        for partition_name in self.partition_names:
            self._partitions[partition_name] = Partition(
                name=partition_name,
                weight=self.partition_weights.get(partition_name, PartitionManager.DEFAULT_WEIGHT),
                seed=self.partition_seeds.get(partition_name, partition_name),
            )

    def _compute_weighted_score(self, partition: Partition, key: str) -> float:
        hash_key = self._hash(partition.seed, key)
        hash_f = self._int_to_float(hash_key)
        score = 1.0 / -math.log(hash_f)
        return partition.weight * score

    @staticmethod
    def _int_to_float(value: int) -> float:
        """
        Converts a uniformly random [[64-bit computing|64-bit]] integer to uniformly random
        floating point number on interval <math>[0, 1)</math>.
        """
        fifty_three_ones = 0xFFFFFFFFFFFFFFFF >> (64 - 53)
        fifty_three_zeros = float(1 << 53)
        return (value & fifty_three_ones) / fifty_three_zeros

    @staticmethod
    def _hash(seed: str, key: str) -> int:
        md5 = hashlib.md5()
        md5.update(seed.encode("utf-8"))
        md5.update(key.encode("utf-8"))
        return int(md5.hexdigest(), 16)

    def get_partition(self, key: str) -> str:
        """
        Determines which partition, from a set of partitions of various weights, is responsible
        for the provided key.
        """
        if len(self._partitions) == 1:
            return list(self._partitions.values())[0].name

        highest_score: float = -1.0
        champion: Optional[Partition] = None
        for partition in self._partitions.values():
            score = self._compute_weighted_score(partition=partition, key=key)
            if score > highest_score:
                champion = partition
                highest_score = score
        assert champion
        return champion.name

    def get_all_partition_names(self) -> List[str]:
        return list(self._partitions.keys())

    def get_partition_count(self) -> int:
        return len(self._partitions)

    def get_partition_details(self, partition_name: str) -> Optional[Partition]:
        return self._partitions.get(partition_name)

    def get_partitions_str(self) -> List[str]:
        return [str(partition) for partition in self._partitions.values()]
