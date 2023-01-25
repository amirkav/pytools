import math
from abc import ABC, abstractmethod


class PartitionUtilsBase(ABC):
    @abstractmethod
    def get_files_partition_count(self, files_count: int) -> int:
        raise NotImplementedError("Should be implemented in the derived class.")

    @abstractmethod
    def get_users_partition_count(self, users_count: int) -> int:
        raise NotImplementedError("Should be implemented in the derived class.")


class DynamoPartitionUtils(PartitionUtilsBase):
    # The size of these partition is determined by past experience
    # in partitioning `prod` DynamoTable size.
    USERS_PARTITION_SIZE = 5000
    FILES_PARTITION_SIZE = 500_000

    def get_files_partition_count(self, files_count: int) -> int:
        return min(10, math.ceil(files_count / self.FILES_PARTITION_SIZE))

    def get_users_partition_count(self, users_count: int) -> int:
        return math.ceil(users_count / self.USERS_PARTITION_SIZE)
