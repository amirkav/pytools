#!/usr/bin/env python

"""
Generic methods and logic to connect to dynamo db.
Do not include any logic specific to data or a table here.

Guides & Documentation
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
https://docs.aws.amazon.com/cli/latest/reference/dynamodb/index.html#cli-aws-dynamodb
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.html

Using indexes
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.Querying
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-indexes.html

AttributeTypes:
    STRING = 'S'
    NUMBER = 'N'
    BINARY = 'B'
    STRING_SET = 'SS'
    NUMBER_SET = 'NS'
    BINARY_SET = 'BS'
    NULL = 'NULL'
    BOOLEAN = 'BOOL'
    MAP = 'M'
    LIST = 'L'


# ON RETRY AND BACKOFF LOGIC
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff


## 5xx Errors
botocore already implements an exponential backoff,
so when it gives the 5xx errors,
it already did its max tries (max tries can be configured).
So, if you're using boto3, you don't need to retry
original requests that receive server errors (5xx).


See:
https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

If you're not using an AWS SDK such as boto3 ,
you should retry original requests that receive server errors (5xx).



## 4xx Errors
4xx errors depend on the application and the AWS service being used.
However, client errors (4xx, other than a `ThrottlingException`
or a `ProvisionedThroughputExceededException`)
indicate that you need to revise the request itself
to correct the problem before trying again.
So, it's best not to suppress or retry these errors.

But, for 4xx errors that are related to resource usage and
could be fixed by a retry logic, you need to write your own logic.

The issue with boto3 client is that it does not implement different exceptions
as individual classes. So, it is impossible to decide whether to
retry an exception based on its class.
Most boto3 exceptions are simply ClientError; but, within ClientError
we have a large number of errors that do not have to be treated the same.
For instance, 'ProvisionedThroughputExceededException' and 'ThrottlingException'
errors are related to provisioned throughput of the Dynamo table,
and should be retried. But these exceptions are not implemented
as their individual classes, so we cannot simple use them in
`except ... ` statement.
See:
https://github.com/boto/boto3/issues/597
"""
from datetime import datetime as dt
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

from botocore.exceptions import ClientError

from pytools.aws.boto3_connect import Boto3Connect
from pytools.common.class_utils import cached_property
from pytools.common.datetime_utils import UNIX_DATETIME_FORMAT
from pytools.aws.dynamo_autoscale_helper import DynamoAutoscaleHelper, GlobalSecondaryIndex
from pytools.common.logger import Logger
from pytools.aws.retry_backoff_boto3 import BatchUnprocessedItemsError, RetryAndBackoffBoto3
from pytools.common.string_utils import StringUtils

RawAWSResponse = Any


#######################################
class DynamoConnect(Boto3Connect):

    SCAN_PAGE_SIZE = 1000

    @property
    def service(self) -> str:
        return "dynamodb"

    @cached_property
    def app_autoscaling_client(self) -> Any:
        return self.boto3_session.client(
            "application-autoscaling",
            config=self.boto3_config,
            endpoint_url=self.endpoint_url,
            region_name=self.aws_region,
        )

    @cached_property
    def autoscale_helper(self) -> DynamoAutoscaleHelper:
        return DynamoAutoscaleHelper(client=self.app_autoscaling_client)

    @staticmethod
    def _build_key_schema(
        partition_key_name: str,
        partition_key_type: str,
        sort_key_name: str = None,
        sort_key_type: str = None,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        key_schema = []
        attribute_defs = []

        # add partition key
        key_schema.extend([{"AttributeName": partition_key_name, "KeyType": "HASH"}])
        attribute_defs.extend(
            [{"AttributeName": partition_key_name, "AttributeType": partition_key_type}]
        )

        # add sort key, if provided by the user
        if sort_key_name is not None and sort_key_type is not None:
            key_schema.extend([{"AttributeName": sort_key_name, "KeyType": "RANGE"}])
            attribute_defs.extend(
                [{"AttributeName": sort_key_name, "AttributeType": sort_key_type}]
            )

        return key_schema, attribute_defs

    #######################################

    def dedupe_parameters_list(
        self, parameters_list: List[Dict[str, Any]], key: str
    ) -> List[Dict[str, Any]]:
        unique_parameters_list = []
        unique_keys: Set[Any] = set()
        for parameter in parameters_list:
            key_name = parameter.get(key)
            if key_name in unique_keys:
                self._logger.warning(f"Duplicate key {key} = {key_name}")
                continue

            unique_parameters_list.append(parameter)
            unique_keys.add(key_name)

        return unique_parameters_list

    #######################################

    def create_table(
        self,
        table_name: str,
        partition_key_name: str,
        partition_key_type: str,
        sort_key_name: str = None,
        sort_key_type: str = None,
        tags: List[Dict[str, Any]] = None,
        global_secondary_indexes: Iterable[GlobalSecondaryIndex] = (),
        local_secondary_indexes: List[Dict[str, Any]] = None,
        throughput_read_capacity_units: int = 50,
        throughput_write_capacity_units: int = 10,
    ) -> Optional[RawAWSResponse]:
        """Create a table in dynamodb
        https://medium.com/@cols.knil/autoscaling-in-dynamodb-with-boto3-bf5bbeb99b10
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table

        :param str table_name:
        :param str partition_key_name:
        :param str partition_key_type: See AttributeTypes in file docstring
        :param str sort_key_name:
        :param str sort_key_type: See AttributeTypes in file docstring
        :param list global_secondary_indexes: for format, see:
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
            services/dynamodb.html#DynamoDB.ServiceResource.create_table
        :param list tags: format:
            [
                {
                    'Key': 'string',
                    'Value': 'string'
                },
            ]
        :returns dict: Raw aws response
        """
        if self.table_exists(table_name):
            self._logger.warning(f"Table {table_name} already exists.")
            return None

        key_schema, attribute_defs = self._build_key_schema(
            partition_key_name, partition_key_type, sort_key_name, sort_key_type
        )

        #########
        # add global and local secondary indexes
        extra_params: Dict[str, Any] = {}

        if global_secondary_indexes:
            for gsi in global_secondary_indexes:
                gsi_key_schema = gsi["KeySchema"]
                for key in gsi_key_schema:
                    gsi_attributes = [dict(AttributeName=key["AttributeName"], AttributeType="S")]
                    attribute_defs.extend(gsi_attributes)
            for gsi in global_secondary_indexes:
                gsi.update(
                    dict(
                        ProvisionedThroughput={
                            "ReadCapacityUnits": throughput_read_capacity_units,
                            "WriteCapacityUnits": throughput_write_capacity_units,
                        }
                    )
                )
            extra_params.update(dict(GlobalSecondaryIndexes=global_secondary_indexes))

        if local_secondary_indexes is not None:
            for lsi in local_secondary_indexes:
                lsi_key_schema = lsi["KeySchema"]
                for key in lsi_key_schema:
                    lsi_attributes = [dict(AttributeName=key["AttributeName"], AttributeType="S")]
                    attribute_defs.extend(lsi_attributes)
            extra_params.update(dict(LocalSecondaryIndexes=local_secondary_indexes))

        if tags is None:
            tags = []

        extra_params["Tags"] = self.dedupe_parameters_list(tags, key="Key")

        #########
        unique_attribute_defs = self.dedupe_parameters_list(attribute_defs, key="AttributeName")
        # create the table
        response = self.client.create_table(
            TableName=table_name,
            KeySchema=key_schema,
            AttributeDefinitions=unique_attribute_defs,
            ProvisionedThroughput={
                "ReadCapacityUnits": throughput_read_capacity_units,
                "WriteCapacityUnits": throughput_write_capacity_units,
            },
            **extra_params,
        )

        # enable autoscaling
        self.register_auto_scaling(table_name, global_secondary_indexes)

        # wait until the table is created, and then return
        self._logger.info(f"Table {table_name} is being created.")
        waiter = self.client.get_waiter("table_exists")
        waiter.wait(TableName=table_name, WaiterConfig={"Delay": 10, "MaxAttempts": 12})
        self._logger.info(f"Table {table_name} created successfully.")

        return response

    def register_auto_scaling(
        self,
        table_name: str,
        global_secondary_indexes: Iterable[GlobalSecondaryIndex] = (),
        min_capacity: int = DynamoAutoscaleHelper.SCALE_MIN_CAPACITY,
        max_capacity: int = DynamoAutoscaleHelper.SCALE_MAX_CAPACITY,
    ) -> None:
        """
        Register auto scaling for table
        :param str table_name: the name of the table
        :param list global_secondary_indexes: indexes that should also have autoscaling
        :param str min_capacity: MinCapacity for table and indexes
        :param str max_capacity: MaxCapacity for table and indexes
        :return dict: raw AWS response
        """
        self.autoscale_helper.register_auto_scaling(
            table_name,
            global_secondary_indexes,
            min_capacity=min_capacity,
            max_capacity=max_capacity,
        )

    def deregister_auto_scaling(
        self, table_name: str, global_secondary_indexes: Iterable[GlobalSecondaryIndex] = ()
    ) -> None:
        """
        Deregister auto scaling for table
        :param str table_name: the name of the table
        :param list global_secondary_indexes: indexes that should have autoscaling disabled
        :return dict: raw AWS response
        """
        self.autoscale_helper.deregister_auto_scaling(table_name, global_secondary_indexes)

    #######################################
    def add_gsi(self, table_name: str) -> None:
        # https://stackoverflow.com/questions/49889790/how-to-add-a-dynamodb-global-secondary-index-via-python-boto3
        raise NotImplementedError

    #######################################

    def describe_table(self, table_name: str) -> RawAWSResponse:
        response = self.client.describe_table(TableName=table_name)
        response["Table"]["CreationDateTime"] = dt.strftime(
            response["Table"]["CreationDateTime"], UNIX_DATETIME_FORMAT
        )
        self._logger.json(dict(response))
        return response

    #######################################
    def get_table_key(self, table_name: str) -> None:
        """
        returns a dict of table's key
        This is used to provide meaningful logs and feedback to the user or
        automate the task of creating a new item.
        :param str table_name: the name of the table
        :return: dict
        """
        raise NotImplementedError

    #######################################

    def table_exists(self, table_name: str) -> bool:
        existing_tables = self.client.list_tables()["TableNames"]
        return table_name in existing_tables

    #######################################

    def delete_table(self, tab_name: str) -> RawAWSResponse:
        """Delete a table
        :param str table_name:
        :returns dict: Raw aws response
        """
        table = self.resource.Table(tab_name)
        self._logger.info(f"Table {tab_name} is being deleted.")
        try:
            response = table.delete()
        except self.client.exceptions.ResourceNotFoundException as e:
            self._logger.exception(e, level=Logger.WARNING)
            return 1

        # wait until the table is created, and then return
        waiter = self.client.get_waiter("table_not_exists")
        waiter.wait(TableName=tab_name, WaiterConfig={"Delay": 10, "MaxAttempts": 6})
        self._logger.info(f"Table {tab_name} deleted successfully.")

        return response

    # UPDATE & PUT DATA
    def put_item(self, table_name: str, item_dict: Dict[str, Any]) -> RawAWSResponse:
        """Adds or REPLACES an item in dynamo.
        When you add an item, the primary key attribute(s) are the only required attributes.
        Attribute values cannot be null.
        String and Binary type attributes must have lengths greater than zero.
        Set type attributes cannot be empty.
        Requests with empty values will be rejected with a ValidationException exception.

        If you overwrite an existing item, ReturnValues='ALL_OLD' returns
        the entire item as it appeared before the overwrite.

        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html#GettingStarted.Python.03.01
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item

        :param str table_name:
        :param dict item_dict: Full item you wish to store in dynamo.
            For the item dictionary, primary key attribute(s) are the only required attributes.
        :returns dict: Raw aws response
        """
        table = self.resource.Table(table_name)
        response = table.put_item(Item=item_dict, ReturnValues="ALL_OLD")

        return response

    #######################################
    def update_item(
        self,
        table_name: str,
        key_dict: Dict[str, str],
        update_expression: str,
        condition_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, str]] = None,
        return_values: str = "UPDATED_NEW",
    ) -> RawAWSResponse:
        """Modifies an existing item.
        update-expression ::=
            [ SET action [, action] ... ]
            [ REMOVE action [, action] ...]
            [ ADD action [, action] ... ]
            [ DELETE action [, action] ...]

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_item
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html#GettingStarted.Python.03.03

        :param str table_name:
        :param dict key_dict: Dictionary of keys which should identify an item to update
        :param str update_expression: See docs linked above
        :param dict expression_attribute_names: Dictionary containing any attribute names
        :param dict expression_attribute_values: Dictionary contianing any attribute values
        :returns dict: Raw aws response
        """
        extra_params: Dict[str, Any] = {}
        if expression_attribute_names is not None:
            extra_params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values is not None:
            extra_params["ExpressionAttributeValues"] = expression_attribute_values
        if condition_expression is not None:
            extra_params["ConditionExpression"] = condition_expression

        table = self.resource.Table(table_name)

        response = table.update_item(
            Key=key_dict,
            UpdateExpression=update_expression,
            ReturnValues=return_values,
            ReturnConsumedCapacity="TOTAL",
            **extra_params,
        )

        return response

    #######################################
    def increment_attribute(
        self, table_name: str, key_dict: Dict[str, str], incr_attr: str, incr_val: Any
    ) -> RawAWSResponse:
        """
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
            services/dynamodb.html#DynamoDB.Table.update_item
        - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
            WorkingWithItems.html#WorkingWithItems.AtomicCounters
        :param table_name:
        :param key_dict:
        :param incr_attr:
        :param incr_val:
        :return:
        """
        table = self.resource.Table(table_name)
        response = table.update_item(
            Key=key_dict,
            UpdateExpression="set #a = #a + :incr_val",
            ExpressionAttributeValues={":incr_val": incr_val},
            ExpressionAttributeNames={"#a": incr_attr},
            ReturnValues="UPDATED_NEW",
        )

        return response

    #######################################
    @RetryAndBackoffBoto3()
    def batch_write_item(
        self, request_items: Any, previous_responses: Optional[Iterable[RawAWSResponse]] = None
    ) -> RawAWSResponse:
        """
        The BatchWriteItem operation can contain up to 25 individual
        PutItem and DeleteItem requests and can write up to 16 MB of data.
        The maximum size of an individual item is 400 KB.

        BatchWriteItem does not support UpdateItem requests.

        A BatchWriteItem operation can put or delete items in multiple tables.


        # ERROR HANDLING AND VALIDATION
        A batch operation does not fail unless all of the requests in the batch fail.

        If any requested operations fail because the table's
        provisioned throughput is exceeded or an internal processing failure occurs,
        the failed operations are returned in the 'UnprocessedItems' response parameter.
        You can investigate and optionally resend the requests.

        Typically, you would call BatchWriteItem in a loop.
        Each iteration would check for unprocessed items and submit
        a new BatchWriteItem request with those unprocessed items
        until all items have been processed.
        If DynamoDB returns any unprocessed items,
        it is best practice to use an exponential backoff algorithm for retrying.

        If none of the items can be processed due to
        insufficient provisioned throughput on all of the
        tables in the request, then BatchWriteItem
        returns a ProvisionedThroughputExceededException.


        If one or more of the following is true,
          DynamoDB rejects the entire batch write operation:
        - One or more tables specified in the BatchWriteItem request does not exist.
        - Primary key attributes specified on an item in the request
          do not match those in the corresponding table's primary key schema.
        - You try to perform multiple operations on the same item
          in the same BatchWriteItem request.
          For example, you cannot put and delete the same item
          in the same BatchWriteItem request.
        - Your request contains at least two items with
          identical hash and range keys (which essentially is two put operations).
        - There are more than 25 requests in the batch.
        - Any individual item in a batch exceeds 400 KB.
        - The total request size exceeds 16 MB.


        LIMITATIONS
        - We cannot specify conditions on individual put and delete requests.
        - BatchWriteItem does not return deleted items in the response.


        - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
            WorkingWithItems.html#WorkingWithItems.BatchOperations
        - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
            services/dynamodb.html#DynamoDB.Client.batch_write_item
        - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
            ErrorHandling.html#Programming.Errors.BatchOperations
        :return dict: Raw AWS reponse with PreviousResponses list
        """
        response = self.resource.batch_write_item(
            RequestItems=request_items,
            ReturnConsumedCapacity="TOTAL",
            ReturnItemCollectionMetrics="SIZE",
        )

        if response.get("UnprocessedItems"):
            raise BatchUnprocessedItemsError(
                unprocessed_items=response["UnprocessedItems"], response=response
            )

        response["PreviousResponses"] = previous_responses or []
        return response

    def get_item(
        self,
        table_name: str,
        key_dict: Dict[str, str],
        projection_expression: str = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        consistent_read: bool = False,
    ) -> RawAWSResponse:
        """
        Gets a single item using exact matches.
        GetItem provides an eventually consistent read by default.
        If your application requires a strongly consistent read,
        set ConsistentRead to true .
        Although a strongly consistent read might take more time
        than an eventually consistent read,
        it always returns the last updated value.

        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.get_item
        :param str table_name:
        :param dict key_dict: {'my_prim_key': 123}
        :param list attributes_to_get: a list of strings, representing the attributes to get
        :returns dict: Items from aws response
        """
        extra_params: Dict[str, Any] = {}
        if projection_expression is not None:
            extra_params.update(dict(ProjectionExpression=projection_expression))
        if expression_attribute_names is not None:
            extra_params.update(dict(ExpressionAttributeNames=expression_attribute_names))

        table = self.resource.Table(table_name)

        try:
            response = table.get_item(
                Key=key_dict,
                ConsistentRead=consistent_read,
                ReturnConsumedCapacity="TOTAL",
                **extra_params,
            )

        except ClientError as e:
            self._logger.debug(e.response["Error"]["Message"])
            raise

        else:
            item = response.get("Item", {})

        return item

    #######################################

    @RetryAndBackoffBoto3()
    def batch_get_item(
        self, request_items: Any, previous_responses: Optional[Iterable[RawAWSResponse]] = None
    ) -> RawAWSResponse:
        """
        A single BatchGetItem operation can contain up to 100 individual
            GetItem requests and can retrieve up to 16 MB of data.

        A BatchGetItem operation can retrieve items from multiple tables.

        A batch operation does not fail unless all of the requests in the batch fail.

        BatchGetItem returns a partial result if the response size limit
        is exceeded, the table's provisioned throughput is exceeded,
        or an internal processing failure occurs.

        If a partial result is returned, the operation returns a value for 'UnprocessedKeys'.
        You can use this value to retry the operation starting with the next item to get.
        For retrying, it is best practice to use an exponential backoff algorithm.

        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.batch_get_item
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ErrorHandling.html#Programming.Errors.BatchOperations
        :param dict request_items: A map of one or more table names and,
            for each table, a map that describes one or more items
            to retrieve from that table.
        :return dict: Raw AWS reponse with proper Responses key
        """
        response = self.resource.batch_get_item(
            RequestItems=request_items, ReturnConsumedCapacity="TOTAL"
        )

        if response.get("UnprocessedKeys"):
            raise BatchUnprocessedItemsError(
                unprocessed_items=response["UnprocessedKeys"], response=response
            )

        if previous_responses:
            for previous_response in previous_responses:
                for table_name in previous_response["Responses"]:
                    if table_name not in response["Responses"]:
                        response["Responses"][table_name] = []
                    response["Responses"][table_name] += previous_response["Responses"][table_name]

        return response

    #######################################

    @staticmethod
    def _get_projection_mapping(keys: Iterable[str]) -> Dict[str, str]:
        """
        Build a projection mapping suitable for
        `aws dynamodb ... --expression-attribute-names` value.

        ```python
        keys = ['key1', 'key2']
        projection_mapping = DynamoConnect._get_projection_mapping(keys)
        projection_mapping # {'#aaa': 'key1', '#aab': 'key2'}

        expression_attribute_names = projection_mapping
        expression_attribute_names # {'#aaa': 'key1', '#aab': 'key2'}
        projection_expression = ', '.join(projection_mapping)
        projection_expression # '#aaa, #aab'
        ```

        Arguments:
            keys -- List of attribute names

        Returns:
            A mapping of an alias to an attribute name.
        """
        string_generator = StringUtils.ascii_string_generator(length=3)
        return {f"#{next(string_generator)}": k for k in keys}

    @staticmethod
    def _get_format_dict(
        projection_mapping: Dict[str, str], data_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare format dict to be used for formatting query expressions.

        ```python
        format_dict = dynamo_connect._get_format_dict(
            {'#aaa': 'key', '#aab': 'key2'},
            {
                'key': ['value1', 'value2'],
                'key2': 'value3',
            }
        )
        format_dict
        # {
        #     'key': '#aaa',
        #     'key2': '#aab',
        #     'key__value': ':key_1, :key_2',
        #     'key2__value': ':key2',
        # }
        ```

        Arguments:
            projection_mapping -- Projection mapping built from query expressions.
            data_dict -- Data for query.

        Returns:
            A dict with format keys and values.
        """
        result = {v: k for k, v in projection_mapping.items()}
        for key, value in data_dict.items():
            if isinstance(value, list):
                result[f"{key}__value"] = ", ".join([f":{key}_{i}" for i in range(len(value))])
                continue
            result[f"{key}__value"] = f":{key}"

        return result

    @staticmethod
    def _get_expression_attribute_values(data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build `expression_attribute_values` for queries.

        ```python
        expression_attribute_values = dynamo_connect._get_expression_attribute_values(
            {
                'key': ['value1', 'value2'],
                'key2': 'value3',
            }
        )
        expression_attribute_values
        # {
        #     ':key_1': 'value1',
        #     ':key_2': 'value2',
        #     ':key2': 'value3',
        # }
        ```

        Arguments:
            data_dict -- Data for query.

        Returns:
            A value to be used in ExpressionAttributeValues
        """
        result = {}
        for key, value in data_dict.items():
            if isinstance(value, list):
                for index, item in enumerate(value):
                    result[f":{key}_{index}"] = item
                continue
            result[f":{key}"] = value

        return result

    def query(
        self,
        table_name: str,
        key_condition_expression: str,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, str]] = None,
        filter_expression: Optional[str] = None,
        projection_expression: Optional[str] = None,
        index_name: Optional[str] = None,
        limit: Optional[int] = None,
        reverse: bool = False,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> RawAWSResponse:
        """Returns a list of items matching given conditions
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SQLtoNoSQL.ReadData.Query.html
        Note: The Boto 3 SDK constructs a ConditionExpression for you when you
              use the Key and Attr functions imported from boto3.dynamodb.conditions.
              You can also specify a ConditionExpression as a string.
              https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
        DynamoDB conditions that are supported by boto3:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#dynamodb-conditions
        Conditions Expressions:
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html

        :param str table_name:
        :param Key key_condition_expression: See link above
        :param str expression_attribute_names: See link above
        :param dict expression_attribute_values: See link above
        :param str filter_expression: See link above
        :param str projection_expression: specifies a condition that returns only items that
            satisfy the condition.
        :param str index_name: See link above
        :param int limit: See link above, set Limit
        :param bool reverse: See link above, set ScanIndexForward: "false"
        :param dict extra_params: See link above, additional parameters to apply
        :returns dict: Items from aws response
        """
        # passing a "None" value to dynamo query() method will fail the call.
        # So, we need to use a placeholder dict like 'extra_params' to pass the values,
        # if they are provided.
        if extra_params is None:
            extra_params = {}

        if projection_expression is not None:
            extra_params["ProjectionExpression"] = projection_expression

        if expression_attribute_names is not None:
            extra_params["ExpressionAttributeNames"] = expression_attribute_names

        if expression_attribute_values is not None:
            extra_params["ExpressionAttributeValues"] = expression_attribute_values
        if filter_expression is not None:
            extra_params["FilterExpression"] = filter_expression

        if index_name is not None:
            extra_params["IndexName"] = index_name

        if limit is not None:
            extra_params["Limit"] = limit

        if reverse:
            extra_params["ScanIndexForward"] = "false"

        table = self.resource.Table(table_name)
        response = table.query(KeyConditionExpression=key_condition_expression, **extra_params)

        return response["Items"]

    #######################################

    def scan(
        self,
        table_name: str,
        filter_expression: Optional[str] = None,
        projection_expression: Optional[str] = None,
        expression_attribute_names: Optional[Dict[str, str]] = None,
        expression_attribute_values: Optional[Dict[str, str]] = None,
    ) -> RawAWSResponse:
        """Scans an entire table. WARNING, this is very expensive.
        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
        :param str table_name:
        :param dict key_dict:
        :param Key filter_expression: specifies a condition that returns only items that
            satisfy the condition. Use a Key object.
        :param str projection_expression: specifies a condition that returns only items that
            satisfy the condition.
        :param dict expression_attribute_names: Dictionary containing any attribute names
        :param dict expression_attribute_values: Dictionary contianing any attribute values
        :returns dict: Raw aws response
        """
        table = self.resource.Table(table_name)

        response = table.scan(
            ProjectionExpression=projection_expression,
            FilterExpression=filter_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            Limit=self.SCAN_PAGE_SIZE,
        )
        items = response["Items"]

        while "LastEvaluatedKey" in response:
            response = table.scan(
                ProjectionExpression=projection_expression,
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                Limit=self.SCAN_PAGE_SIZE,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            items.extend(response["Items"])

        return items

    #######################################
    def paginated_scan(
        self,
        table_name: str,
        filter_expression: str = None,
        projection_expression: str = None,
        expression_attribute_names: Dict[str, str] = None,
        expression_attribute_values: Dict[str, str] = None,
    ) -> Iterator[Dict[str, Any]]:
        table = self.resource.Table(table_name)

        response = table.scan(
            ProjectionExpression=projection_expression,
            FilterExpression=filter_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            Limit=self.SCAN_PAGE_SIZE,
        )
        for item in response["Items"]:
            yield item

        # This is where we handle pagination
        while "LastEvaluatedKey" in response:
            response = table.scan(
                ProjectionExpression=projection_expression,
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
                Limit=self.SCAN_PAGE_SIZE,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )
            for item in response["Items"]:
                yield item

    #######################################

    def print_item(self, item: Any) -> None:
        self._logger.json(item)

    #######################################

    def list_len(self, table_name: str, item_key: Dict[str, str], attribute_name: str) -> int:
        item = self.get_item(table_name, item_key)
        return len(item.get(attribute_name))

    #######################################

    def list_first_element(self, table_name: str, item_key: str, attribute_name: str) -> Any:
        items = self.query(table_name, item_key, projection_expression=attribute_name, limit=1)
        if not items:
            raise ValueError(f"Item {item_key} not found in {table_name}")
        return items[0]

    #######################################

    def list_last_element(self, table_name: str, item_key: str, attribute_name: str) -> Any:
        items = self.query(
            table_name, item_key, projection_expression=attribute_name, reverse=True, limit=1
        )
        if not items:
            raise ValueError(f"Item {item_key} not found in {table_name}")
        return items[0]

    def delete_item(
        self,
        table_name: str,
        key_dict: Dict[str, str],
        conditional_expression: str = None,
        expression_attribute_names: Dict[str, str] = None,
        expression_attribute_values: Dict[str, str] = None,
    ) -> Dict[str, Any]:
        """Removes an item
        :param str table_name:
        :param dict key_dict:
        :param str conditional_expression:
        :param dict expression_attribute_names: Dictionary containing any attribute names
        :param dict expression_attribute_values: Dictionary contianing any attribute values
        :returns dict: Raw aws response
        """
        extra_params: Dict[str, Any] = {}
        if conditional_expression is not None:
            extra_params["ConditionExpression"] = conditional_expression
        if expression_attribute_names is not None:
            extra_params["ExpressionAttributeNames"] = expression_attribute_names
        if expression_attribute_values is not None:
            extra_params["ExpressionAttributeValues"] = expression_attribute_values

        table = self.resource.Table(table_name)
        response = table.delete_item(Key=key_dict, ReturnValues="ALL_OLD", **extra_params)

        return response


#######################################
def main() -> None:
    region = "us-west-2"
    table_name = "test_8"
    partition_key_name = "pk"
    partition_key_type = "S"
    sort_key_name = "sk"
    sort_key_type = "S"
    logger = Logger.main(level=Logger.DEBUG)

    dyn_connect = DynamoConnect(aws_region=region)
    table_exists = dyn_connect.table_exists(table_name)
    logger.debug("table_exists: {}".format(table_exists))

    # Create Table
    dyn_connect.create_table(
        table_name,
        partition_key_name=partition_key_name,
        partition_key_type=partition_key_type,
        sort_key_name=sort_key_name,
        sort_key_type=sort_key_type,
        global_secondary_indexes=[
            {
                "IndexName": "gsi_test",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "text", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            },
        ],
        tags=[{"Key": "env", "Value": "dev"}],
    )

    table_details = dyn_connect.describe_table(table_name)
    logger.debug(table_details.get("Table").get("KeySchema"))
    logger.debug(table_details.get("Table").get("ItemCount"))

    # Put Item
    item_dict = {"pk": "1", "sk": "a", "attr": "original attribute value"}
    r = dyn_connect.put_item(table_name, item_dict=item_dict)
    logger.debug(f"put_item() response: {r}")

    # add a second item
    item_dict = {"pk": "2", "sk": "a", "attr": "original attribute value 2"}
    r = dyn_connect.put_item(table_name, item_dict=item_dict)

    # Test update_item
    key_dict_1 = {"pk": "1", "sk": "a"}
    r = dyn_connect.update_item(
        table_name,
        key_dict=key_dict_1,
        update_expression="SET #a = :i",
        expression_attribute_names={"#a": "attr"},
        expression_attribute_values={":i": "updated attribute value"},
    )
    logger.debug(f"update_item() response: {r}")

    # Test get_item
    item = dyn_connect.get_item(table_name, key_dict=key_dict_1)
    assert item
    logger.debug(f"get_item() response: {r}")

    # Test batch get_item
    key_dict_1 = {"pk": "1", "sk": "a"}
    key_dict_2 = {"pk": "2", "sk": "a"}
    request_items = {table_name: {"Keys": [key_dict_1, key_dict_2]}}
    r = dyn_connect.batch_get_item(request_items)
    logger.debug(f"batch_get_item() response: {r}")

    # Test batch write item
    item_dict = {"pk": "3", "sk": "c", "attr": "original attribute value 2"}
    request_items_write = {
        table_name: [{"DeleteRequest": {"Key": key_dict_2}}, {"PutRequest": {"Item": item_dict}}]
    }
    r = dyn_connect.batch_write_item(request_items_write)
    logger.debug(f"batch_write_item() response: {r}")

    # Test delete table
    # dyn_connect.delete_table(table_name)

    # # Test query
    # # Get all items based on a primary key
    # logger.debug('Test query_item')
    # primary_key = 'pk'
    # sort_key = 'sk'
    # items = dyn_connect.query(
    #     table_name,
    #     Key(primary_key).eq(1),
    # )
    # assert items
    # # Get all items based on primary and secondary key between values
    # items = dyn_connect.query(
    #     table_name,
    #     Key(primary_key).eq(1) & Key(sort_key).between(6, 8)
    # )
    # assert items

    # # items = dyn_connect.query(
    # #     table_name,
    # #     "pk = 1 AND sk > 6 AND sk < 8"
    # # )

    # items = dyn_connect.scan(table_name)
    # items = dyn_connect.paginated_scan(table_name)

    # # Delete table if it exists


if __name__ == "__main__":
    main()
