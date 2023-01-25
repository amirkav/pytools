from typing import Any, Dict, Iterable, Optional

from pytools.common.logger import Logger
from pytools.type_defs import Literal, TypedDict

KeySchemaKey = TypedDict(
    "KeySchemaKey", {"AttributeName": str, "KeyType": Literal["HASH", "RANGE"]}
)
ProvisionedThroughput = TypedDict(
    "ProvisionedThroughput", {"ReadCapacityUnits": int, "WriteCapacityUnits": int}
)
Projection = TypedDict("Projection", {"ProjectionType": Literal["ALL", "KEYS_ONLY", "INCLUDE"]})
GlobalSecondaryIndex = TypedDict(
    "GlobalSecondaryIndex",
    {
        "IndexName": str,
        "KeySchema": Iterable[KeySchemaKey],
        "Projection": Projection,
        "ProvisionedThroughput": ProvisionedThroughput,
    },
    total=False,
)
ScalableDimension = Literal[
    "dynamodb:table:ReadCapacityUnits",
    "dynamodb:table:WriteCapacityUnits",
    "dynamodb:index:ReadCapacityUnits",
    "dynamodb:index:WriteCapacityUnits",
]
MetricType = Literal["DynamoDBReadCapacityUtilization", "DynamoDBWriteCapacityUtilization"]


class DynamoAutoscaleHelper:
    """
    Helper that handles registration and deregistration of auto scaling for DynamoDB
    tables and indexes.
    """

    SCALE_TARGET_VALUE = 50.0  # percent
    SCALE_OUT_COOLDOWN = 60  # seconds
    SCALE_IN_COOLDOWN = 120  # seconds

    SCALE_MIN_CAPACITY = 50
    SCALE_MAX_CAPACITY = 40000

    METRIC_TYPE_READ = "read"
    METRIC_TYPE_WRITE = "write"

    def __init__(self, client: Any) -> None:
        self.client = client
        self._logger = Logger(__name__)

    def deregister_auto_scaling(
        self, table_name: str, global_secondary_indexes: Iterable[GlobalSecondaryIndex] = ()
    ) -> None:
        """
        Deregister auto scaling for table
        :param str table_name: the name of the table
        :param list global_secondary_indexes: indexes that should have autoscaling disabled
        :return dict: raw AWS response
        """
        for index_name in [gsi.get("IndexName") for gsi in global_secondary_indexes]:
            self.deregister_scalable_target(
                table_name=table_name,
                scalable_dimension="dynamodb:index:ReadCapacityUnits",
                index_name=index_name,
            )
            self.deregister_scalable_target(
                table_name=table_name,
                scalable_dimension="dynamodb:index:WriteCapacityUnits",
                index_name=index_name,
            )

        self.deregister_scalable_target(
            table_name=table_name, scalable_dimension="dynamodb:table:ReadCapacityUnits"
        )
        self.deregister_scalable_target(
            table_name=table_name, scalable_dimension="dynamodb:table:WriteCapacityUnits"
        )

    def register_auto_scaling(
        self,
        table_name: str,
        global_secondary_indexes: Iterable[GlobalSecondaryIndex] = (),
        min_capacity: int = SCALE_MIN_CAPACITY,
        max_capacity: int = SCALE_MAX_CAPACITY,
    ) -> None:
        """
        Register auto scaling for table
        :param str table_name: the name of the table
        :param list global_secondary_indexes: indexes that should also have autoscaling
        :param str min_capacity: MinCapacity for table and indexes
        :param str max_capacity: MaxCapacity for table and indexes
        :return dict: raw AWS response
        """
        self._logger.debug(f"Registering Read scalable target for {table_name} table")
        self.register_scalable_target(
            table_name,
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            min_capacity=min_capacity,
            max_capacity=max_capacity,
        )
        self.put_scaling_policy(
            table_name,
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            scaling_policy_configs=self.create_scaling_policy_configs(
                "DynamoDBReadCapacityUtilization"
            ),
        )

        self._logger.debug(f"Registering Write scalable target for {table_name} table")
        self.register_scalable_target(
            table_name,
            scalable_dimension="dynamodb:table:WriteCapacityUnits",
            min_capacity=min_capacity,
            max_capacity=max_capacity,
        )
        self.put_scaling_policy(
            table_name,
            scalable_dimension="dynamodb:table:WriteCapacityUnits",
            scaling_policy_configs=self.create_scaling_policy_configs(
                "DynamoDBWriteCapacityUtilization"
            ),
        )

        # For GSIs
        for index_name in [gsi.get("IndexName") for gsi in global_secondary_indexes]:
            self._logger.debug(
                f"Registering Read scalable target for {table_name} table index {index_name}"
            )
            self.register_scalable_target(
                table_name,
                scalable_dimension="dynamodb:index:ReadCapacityUnits",
                index_name=index_name,
                min_capacity=min_capacity,
                max_capacity=max_capacity,
            )
            self.put_scaling_policy(
                table_name,
                index_name=index_name,
                scalable_dimension="dynamodb:index:ReadCapacityUnits",
                scaling_policy_configs=self.create_scaling_policy_configs(
                    "DynamoDBReadCapacityUtilization"
                ),
            )

            self._logger.debug(
                f"Registering Write scalable target for {table_name} table index {index_name}"
            )
            self.register_scalable_target(
                table_name,
                scalable_dimension="dynamodb:index:WriteCapacityUnits",
                index_name=index_name,
                min_capacity=min_capacity,
                max_capacity=max_capacity,
            )
            self.put_scaling_policy(
                table_name,
                index_name=index_name,
                scalable_dimension="dynamodb:index:WriteCapacityUnits",
                scaling_policy_configs=self.create_scaling_policy_configs(
                    "DynamoDBWriteCapacityUtilization"
                ),
            )

    def deregister_scalable_target(
        self,
        table_name: str,
        scalable_dimension: ScalableDimension,
        index_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Deregister scalable table or index
        :param str table_name: the name of the table
        :param str scalable_dimension: scalable dimension name
        :param str index_name: the name of the index. If provided - deregiters policy for index
        :returns dict: Raw aws response
        """
        resource_id = f"table/{table_name}"
        if index_name is not None:
            resource_id = f"table/{table_name}/index/{index_name}"

        return self.client.deregister_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
        )

    def register_scalable_target(
        self,
        table_name: str,
        scalable_dimension: ScalableDimension,
        index_name: Optional[str] = None,
        min_capacity: int = SCALE_MIN_CAPACITY,
        max_capacity: int = SCALE_MAX_CAPACITY,
    ) -> Dict[str, Any]:
        """
        Register scalable table or index
        :param str table_name: the name of the table
        :param str scalable_dimension: scalable dimension name
        :param str index_name: the name of the index. If provided - adds policy for index
        :param str min_capacity: MinCapacity
        :param str max_capacity: MaxCapacity
        :returns dict: Raw aws response
        """
        resource_id = f"table/{table_name}"
        if index_name is not None:
            resource_id = f"table/{table_name}/index/{index_name}"

        return self.client.register_scalable_target(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            MinCapacity=min_capacity,
            MaxCapacity=max_capacity,
        )

    @staticmethod
    def create_scaling_policy_configs(
        metric_type: MetricType,
        target_value: float = SCALE_TARGET_VALUE,
        scale_out_cooldown: int = SCALE_OUT_COOLDOWN,
        scale_in_cooldown: int = SCALE_IN_COOLDOWN,
    ) -> Dict[str, Any]:
        """
        Create auto scaling policy dict
        :param str metric_type: METRIC_TYPE_READ or METRIC_TYPE_WRITE
        :param float target_value: percent of use to aim for
        :param int scale_out_cooldown: Scale out cooldown in seconds
        :param int scale_in_cooldown: Scale in cooldown in seconds
        :returns dict: Scaling policy configs to use in put_scaling_policy
        """
        return {
            "TargetValue": target_value,
            "PredefinedMetricSpecification": {"PredefinedMetricType": metric_type},
            "ScaleOutCooldown": scale_out_cooldown,
            "ScaleInCooldown": scale_in_cooldown,
        }

    def put_scaling_policy(
        self,
        table_name: str,
        scalable_dimension: ScalableDimension,
        scaling_policy_configs: Dict[str, Any],
        index_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Add scaling policy for table or for index
        :param str table_name: the name of the table
        :param str scalable_dimension: scalable dimension name
        :param dict scaling_policy_configs: scaling policy configs from AWS docs
        :param str index_name: the name of the index. If provided - adds policy for index
        :returns dict: Raw aws response
        """
        resource_id = f"table/{table_name}"
        if index_name:
            resource_id = f"table/{table_name}/index/{index_name}"

        return self.client.put_scaling_policy(
            ServiceNamespace="dynamodb",
            ResourceId=resource_id,
            PolicyType="TargetTrackingScaling",
            PolicyName="ScaleDynamoDBReadCapacityUtilization",
            ScalableDimension=scalable_dimension,
            TargetTrackingScalingPolicyConfiguration=scaling_policy_configs,
        )
