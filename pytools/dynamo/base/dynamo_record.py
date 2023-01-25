from typing import Any, Optional

from dynamoquery import DynamoDictClass
from dynamoquery.dynamo_table import DynamoTableError


class DynamoRecord(DynamoDictClass):
    SORT_KEY_PREFIX = ""
    NULL = "NULL"

    pk: Optional[str] = None
    sk: Optional[str] = None
    project_id: Optional[str] = None
    dt_created: Optional[str] = None
    dt_modified: Optional[str] = None

    # pylint: disable=no-self-use
    @DynamoDictClass.sanitize_key("project_id")
    def sanitize_key_project_id(self, value: Optional[str], **kwargs: Any) -> Optional[str]:
        """
        Compute and validate `project_id` dict key implicitly.
        """
        project_id: Optional[str] = kwargs.get("project_id")
        if not project_id:
            return value

        if value is not None and project_id != value:
            raise DynamoTableError(
                f"Invalid record with project_id={value}. It should"
                f" be same as manager's project_id={project_id}"
            )

        return project_id

    # pylint: disable=no-self-use
    @DynamoDictClass.sanitize_key("pk")
    def sanitize_key_pk(self, value: Optional[str], **kwargs: Any) -> Optional[str]:
        """
        Compute and validate `pk` dict key implicitly.
        """
        project_id: Optional[str] = kwargs.get("project_id") or self.project_id
        if not project_id:
            return value

        expected = project_id

        if value is not None and value != expected:
            raise DynamoTableError(f"Invalid record with pk={value}, expected {expected}")

        return expected
