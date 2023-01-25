import datetime
import pathlib
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from jsonschema import SchemaError, ValidationError, validate

from pytools import json_utils
from pytools.common.logger import Logger

_cw_emf_schema_path = (
    pathlib.Path(__file__).parent.parent / "./resources/cw_embedded_metrics/cw_emf_schema.json"
)
with _cw_emf_schema_path.open(mode="rb") as f:
    CW_EMF_SCHEMA = json_utils.load(f)

MAX_METRICS = 100
MAX_DIMENSIONS = 9


class CloudwatchEMFError(Exception):
    pass


class MetricUnit(Enum):
    Seconds = "Seconds"
    Microseconds = "Microseconds"
    Milliseconds = "Milliseconds"
    Bytes = "Bytes"
    Kilobytes = "Kilobytes"
    Megabytes = "Megabytes"
    Gigabytes = "Gigabytes"
    Terabytes = "Terabytes"
    Bits = "Bits"
    Kilobits = "Kilobits"
    Megabits = "Megabits"
    Gigabits = "Gigabits"
    Terabits = "Terabits"
    Percent = "Percent"
    Count = "Count"
    BytesPerSecond = "Bytes/Second"
    KilobytesPerSecond = "Kilobytes/Second"
    MegabytesPerSecond = "Megabytes/Second"
    GigabytesPerSecond = "Gigabytes/Second"
    TerabytesPerSecond = "Terabytes/Second"
    BitsPerSecond = "Bits/Second"
    KilobitsPerSecond = "Kilobits/Second"
    MegabitsPerSecond = "Megabits/Second"
    GigabitsPerSecond = "Gigabits/Second"
    TerabitsPerSecond = "Terabits/Second"
    CountPerSecond = "Count/Second"


@dataclass
class EMFMetric:
    name: str
    value: float
    unit: Optional[MetricUnit] = None


@dataclass
class EMFDimension:
    name: str
    value: str


@dataclass
class EMFMetadata:
    key: Any
    value: Any


class CloudWatchMetricManager:
    def __init__(self, namespace: str, service: Optional[str] = None) -> None:
        """
        CloudWatchMetricManager creates metrics asynchronously thanks to CloudWatch Embedded Metric Format
        (EMF). CloudWatch EMF can create up to 100 metrics per EMF object and metrics, dimensions,
        and namespace created via CloudWatchMetricManager will adhere to the schema, will be serialized and
        validated against EMF Schema.

        Arguments:
            namespace (str) -- The namespace of the metric.
            service (Optional str) --  Service name (class name or feature name) dimension

        Raises:
            CloudwatchEMFError -- When metric metric isn't supported by CloudWatch
            CloudwatchEMFError -- When metric object fails EMF schema validation
        """

        self.namespace = namespace
        self.service = service
        #  Dict of metrics under custom namespace to update
        self.metric_set: Dict[str, Dict] = {}
        # Dict of unique identifiers for a metric (Max dimensions == 9 including service)
        self.dimension_set: Dict[str, Any] = {}
        # Dict of high cardinal metadata for metrics object
        self.metadata_set: Dict[str, Any] = {}
        self.logger = Logger.for_object(self)

    def _add_metric(
        self,
        metric_set: Dict[str, Dict],
        name: str,
        value: float,
        unit: Optional[MetricUnit] = None,
    ) -> None:
        metric: Dict = metric_set.get(name, {"Value": []})
        metric["Value"].append(float(value))
        metric["Unit"] = unit.value if unit else "None"
        self.logger.debug(f"Adding metric: {name} with {metric}")
        metric_set[name] = metric

        if len(metric_set) == MAX_METRICS:
            self.logger.debug(
                f"Exceeded maximum of {MAX_METRICS} metrics - Publishing existing metric set"
            )
            self.log_metrics()

    def add_metric(self, name: str, value: float, unit: Optional[MetricUnit] = None) -> None:
        """Adds given metric

        Example
        -------
        **Add given metric using MetricUnit enum**

            metric.add_metric(name="BookingConfirmation", unit=MetricUnit.Count, value=1)

        **Add given metric using plain string as value unit**

            metric.add_metric(name="BookingConfirmation", unit="Count", value=1)

        Arguments:
            name (str) -- Metric name
            unit (MetricUnit) -- MetricUnit object
            value (float) -- Metric value

        Raises:
            CloudwatchEMFError -- When metric unit is not supported by CloudWatch
        """
        self._add_metric(metric_set=self.metric_set, name=name, value=value, unit=unit)

    def add_emf_metric(self, metric: EMFMetric) -> None:
        self._add_metric(
            metric_set=self.metric_set, name=metric.name, value=metric.value, unit=metric.unit
        )

    def _add_dimension(self, dimension_set: Dict[str, Any], name: str, value: str) -> None:
        self.logger.debug(f"Adding dimension: {name}:{value}")

        if len(dimension_set) == MAX_DIMENSIONS:
            raise CloudwatchEMFError(f"Cannot add more than {MAX_DIMENSIONS} dimensions")

        # Cast value to str according to EMF spec
        # Majority of values are expected to be string already, so
        # checking before casting improves performance in most cases
        if isinstance(value, str):
            dimension_set[name] = value
        else:
            dimension_set[name] = str(value)

    def add_dimension(self, name: str, value: str) -> None:
        """Adds given dimension to all metrics

        Example
        -------
        **Add a metric dimensions**

            metric.add_dimension(name="operation", value="confirm_booking")

        Arguments:
            name (str) -- Dimension name
            value (str) -- Dimension value
        """
        self._add_dimension(dimension_set=self.dimension_set, name=name, value=value)

    def add_emf_dimension(self, dimension: EMFDimension) -> None:
        self._add_dimension(
            dimension_set=self.dimension_set, name=dimension.name, value=dimension.value
        )

    def _add_metadata(self, metadata_set: Dict[str, Any], key: Any, value: Any) -> None:
        self.logger.debug(f"Adding metadata: {key}:{value}")

        # Cast key to str according to EMF spec
        # Majority of keys are expected to be string already, so
        # checking before casting improves performance in most cases
        if isinstance(key, str):
            metadata_set[key] = value
        else:
            metadata_set[str(key)] = value

    def add_metadata(self, key: Any, value: Any) -> None:
        """Adds high cardinal metadata for metrics object

        This will not be available during metrics visualization.
        Instead, this will be searchable through logs.

        If you're looking to add metadata to filter metrics, then
        use add_dimensions method.

        Example
        -------
        **Add metrics metadata**

            metric.add_metadata(key="booking_id", value="booking_id")

        Arguments:
            key (any) -- Metadata key
            value (any) -- Metadata value
        """
        self._add_metadata(metadata_set=self.metadata_set, key=key, value=value)

    def add_emf_metadata(self, metadata: EMFMetadata) -> None:
        self._add_metadata(metadata_set=self.metadata_set, key=metadata.key, value=metadata.value)

    def serialize_metric_set(
        self,
        metric_set: Optional[Dict] = None,
        dimension_set: Optional[Dict] = None,
        metadata_set: Optional[Dict] = None,
    ) -> Dict:
        """Serializes metric and dimensions set

        Arguments:
            metric_set (Dict, optional) -- Dictionary of metrics to serialize, by default None
            dimension_set (Dict, optional) -- Dictionary of dimensions to serialize, by default None
            metadata_set (Dict, optional) -- Dictionary of metadata to serialize, by default None

        Example
        -------
        **Serialize metrics into EMF format**

            metrics = CloudWatchMetricManager()
            # ...add metrics, dimensions, namespace
            ret = metrics.serialize_metric_set()

        Returns:
            Serialized metrics following EMF specification

        Raises:
            CloudwatchEMFError -- Raised when serialization fail schema validation
        """

        if metric_set is None:
            metric_set = self.metric_set

        if dimension_set is None:
            dimension_set = self.dimension_set

        if metadata_set is None:
            metadata_set = self.metadata_set

        if self.service and not dimension_set.get("service"):
            dimension_set["service"] = self.service

        self.logger.debug(
            json_utils.dumps(
                {
                    "details": "Serializing metrics",
                    "metrics": metric_set,
                    "dimensions": dimension_set,
                }
            )
        )

        # [ { "Name": "metric_name", "Unit": "Count" } ]
        metric_names_and_units: List[Dict[str, str]] = []
        metric_names_and_values: Dict[str, float] = {}  # { "metric_name": 1.0 }

        for metric_name in metric_set:
            metric: Dict = metric_set[metric_name]
            metric_value: int = metric.get("Value", 0)
            metric_unit: str = metric.get("Unit", "")

            metric_names_and_units.append({"Name": metric_name, "Unit": metric_unit})
            metric_names_and_values.update({metric_name: metric_value})

        embedded_metrics_object: Dict = {
            "_aws": {
                "Timestamp": int(datetime.datetime.now().timestamp() * 1000),  # epoch
                "CloudWatchMetrics": [
                    {
                        "Namespace": self.namespace,
                        "Dimensions": [list(dimension_set.keys())],
                        "Metrics": metric_names_and_units,
                    }
                ],
            },
            **metadata_set,
            **dimension_set,
            **metric_names_and_values,
        }

        try:
            validate(instance=embedded_metrics_object, schema=CW_EMF_SCHEMA)
        except (ValidationError, SchemaError) as e:
            message = f"Error: {e.message}"
            raise CloudwatchEMFError(message) from e

        return embedded_metrics_object

    def log_metrics(
        self,
        metric_set: Optional[Dict] = None,
        dimension_set: Optional[Dict] = None,
        metadata_set: Optional[Dict] = None,
    ) -> None:
        """Logs the EMF metrics"""
        if metric_set is None:
            metric_set = self.metric_set

        if dimension_set is None:
            dimension_set = self.dimension_set

        if metadata_set is None:
            metadata_set = self.metadata_set

        if len(metric_set) == 0:
            return

        metrics = self.serialize_metric_set(
            metric_set=metric_set, dimension_set=dimension_set, metadata_set=metadata_set
        )
        # clear metric set only as opposed to metadata and dimensions set since we could have
        # more than 100 metrics
        metric_set.clear()
        self._log(metrics=metrics)

    def _log(self, metrics: Dict) -> None:
        formatter = self.logger.JSONFormatter(json_utils.dumps(metrics))
        old_formatter = self.logger.formatter
        self.logger.set_formatter(formatter)
        # this line actually can have any message and level, it will still log cw metrics
        self.logger.info("")
        self.logger.set_formatter(old_formatter)

    def log_metrics_with_custom_dimensions(
        self,
        dimensions: List[EMFDimension],
        metrics: List[EMFMetric],
        metadatas: Optional[List[EMFMetadata]] = None,
    ) -> None:
        metric_set: Dict[str, Dict] = {}
        dimension_set: Dict[str, Any] = {}
        metadata_set: Dict[str, Any] = {}

        for dimension in dimensions:
            self._add_dimension(
                dimension_set=dimension_set, name=dimension.name, value=dimension.value
            )

        for meta_dimension in ("project_id", "env", "suffix"):
            if not dimension_set.get(meta_dimension):
                self._add_dimension(
                    dimension_set=dimension_set,
                    name=meta_dimension,
                    value=self.dimension_set[meta_dimension],
                )

        for metric in metrics:
            self._add_metric(
                metric_set=metric_set, name=metric.name, value=metric.value, unit=metric.unit
            )

        if metadatas:
            for metadata in metadatas:
                self._add_metadata(
                    metadata_set=metadata_set, key=metadata.key, value=metadata.value
                )

        self.log_metrics(
            metric_set=metric_set, dimension_set=dimension_set, metadata_set=metadata_set
        )


# if __name__ == "__main__":
#     Logger.main()
#     Logger.main(formatter=Logger.JSONFormatter())
#     mm = CloudWatchMetricManager(namespace="Ingestion", service="Uploader")
#     mm.add_metadata("logger_group", "log")
#     mm.add_metadata("project_id", "test_project123")
#     mm.add_dimension(name="project_id", value="test_project")
#     mm.add_dimension(name="env", value="test_env")
#     mm.add_dimension(name="suffix", value="test_suffix")
#     mm.add_dimension(name="operation", value="confirm_booking")
#     mm.add_metric(name="BookingConfirmation", unit=MetricUnit.Count, value=1)
#     mm.add_metric(name="BookingConfirmation2", unit=MetricUnit.Count, value=5)
#     mm.log_metrics_with_custom_dimensions(
#         dimensions=[
#             EMFDimension("custom_operation1", "custom_confirm_booking1"),
#             EMFDimension("custom_operation2", "custom_confirm_booking2"),
#             EMFDimension("project_id", "custom_project"),
#             EMFDimension("env", "custom_env"),
#             EMFDimension("suffix", "custom_suffix"),
#         ],
#         metrics=[
#             EMFMetric("CustomBookingConfirmation1", 10, MetricUnit.Count),
#             EMFMetric("CustomBookingConfirmation2", 100, MetricUnit.Count),
#         ],
#     )
#     mm.log_metrics()
