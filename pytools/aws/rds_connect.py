#!/usr/bin/env python

from typing import Any, Dict, List, Optional

from pytools.aws.boto3_connect import Boto3Connect

RawAWSResponse = Any


class RdsConnect(Boto3Connect):
    @property
    def service(self) -> str:
        return "rds"

    def describe_all_db_cluster_parameter_groups(self) -> List[Dict[str, Any]]:
        """Gets all AWS RDS db clusteer parameter groups

        Returns:
            List of AWS RDS db cluster parameter groups
        """
        response = self.describe_db_cluster_parameter_groups()
        db_cluster_param_grps = response.get("DBClusterParameterGroups", [])
        while "Marker" in response:
            response = self.describe_db_cluster_parameter_groups(Marker=response.get("Marker"))
            db_cluster_param_grps += response.get("DBClusterParameterGroups")
        return db_cluster_param_grps

    def describe_all_db_parameter_groups(self) -> List[Dict[str, Any]]:
        """Gets all AWS RDS db parameter groups (instance)

        Returns:
             AWS response for describe_db_parameter_groups
        """
        response = self.describe_db_parameter_groups()
        db_param_grps = response.get("DBParameterGroups", [])
        while "Marker" in response:
            response = self.describe_db_parameter_groups(Marker=response.get("Marker"))
            db_param_grps += response.get("DBParameterGroups")
        return db_param_grps

    def describe_db_cluster_parameter_groups(self, **kwargs: Any) -> RawAWSResponse:
        """Gets AWS RDS db clusteer parameter groups

        [boto3 - describe_db_cluster_parameter_groups](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_cluster_parameter_groups
        )

        Returns:
            AWS response for describe_db_cluster_parameter_groups
        """
        return self.client.describe_db_cluster_parameter_groups(**kwargs)

    def describe_db_parameter_groups(self, **kwargs: Any) -> RawAWSResponse:
        """Gets AWS RDS db parameter groups

        [boto3 - describe_db_parameter_groups](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_parameter_groups
        )

        Returns:
            AWS response for describe_db_parameter_groups
        """
        return self.client.describe_db_parameter_groups(**kwargs)

    def create_db_cluster_parameter_group(
        self, cluster_param_grp_name: str, param_grp_family: str, description: str, **kwargs: Any
    ) -> RawAWSResponse:
        """Creates an AWS RDS db cluster parameter group

        [boto3 - create_db_cluster_parameter_group](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_cluster_parameter_group
        )

        Arguments:
            cluster_param_grp_nam -- AWS RDS db cluster parameter group name
            param_grp_family -- AWS RDS db parameter group family
            description -- Description for the AWS RDS db cluster parameter group

        Returns:
            AWS response for create_db_cluster_parameter_group
        """
        return self.client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=cluster_param_grp_name,
            DBParameterGroupFamily=param_grp_family,
            Description=description,
            **kwargs,
        )

    def create_db_parameter_group(
        self, param_grp_name: str, param_grp_family: str, description: str, **kwargs: Any
    ) -> RawAWSResponse:
        """Creates an AWS RDS db parameter group

        [boto3 - create_db_parameter_group](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_parameter_group
        )

        Arguments:
            param_grp_name -- AWS RDS db parameter group name
            param_grp_family -- AWS RDS db parameter group family
            description -- Description for AWS RDS db parameter group

        Returns:
            AWS response for create_db_parameter_group
        """
        return self.client.reate_db_parameter_group(
            DBParameterGroupName=param_grp_name,
            DBParameterGroupFamily=param_grp_family,
            Description=description,
            **kwargs,
        )

    def modify_db_parameter_group(
        self, param_grp_name: str, parameters: List[Dict[str, Any]]
    ) -> RawAWSResponse:
        """Modifies an AWS RDS db parameter group

        [boto3 - modify_db_cluster_parameter_group](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.modify_db_parameter_group
        )

        Arguments:
            param_grp_name -- AWS RDS db parameter group name
            parameters -- AWS RDS db parameter group parameters

        Returns:
            AWS response for modify_db_parameter_group
        """
        return self.client.modify_db_parameter_group(
            DBParameterGroupName=param_grp_name, Parameters=parameters
        )

    def modify_db_cluster_parameter_group(
        self, cluster_param_grp_name: str, parameters: List[Dict[str, Any]]
    ) -> RawAWSResponse:
        """Modifies an AWS RDS db cluster parameter group

        [boto3 - modify_db_cluster_parameter_group](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.modify_db_cluster_parameter_group
        )

        Arguments:
            cluster_param_grp_name -- AWS RDS db cluster group name
            parameters -- AWS RDS db cluster group parameters

        Returns:
            AWS response for modify_db_cluster_parameter_group
        """
        return self.client.modify_db_cluster_parameter_group(
            DBClusterParameterGroupName=cluster_param_grp_name, Parameters=parameters
        )

    def describe_all_db_clusters(self) -> List[Dict[str, Any]]:
        """Gets all AWS RDS db clusters

        Returns:
            List of AWS RDS db clusters
        """
        response = self.describe_db_clusters()
        db_clusters = response.get("DBClusters", [])
        while "Marker" in response:
            response = self.describe_db_clusters(Marker=response.get("Marker"))
            db_clusters += response.get("DBClusters")
        return db_clusters

    def describe_db_clusters(self, **kwargs: Any) -> RawAWSResponse:
        """Gets AWS RDS db cluster

        Returns:
            AWS response for describe_db_clusters
        """
        return self.client.describe_db_clusters(**kwargs)

    def describe_all_db_instances(self) -> List[Dict[str, Any]]:
        """Gets all AWS RDS db instances

        [boto3 - describe_db_instances](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_db_instances
        )

        Returns:
            List of AWS RDS db instance
        """
        response = self.describe_db_instances()
        db_instances = response.get("DBInstances", [])
        while "Marker" in response:
            response = self.describe_db_instances(Marker=response.get("Marker"))
            db_instances += response.get("DBInstances")
        return db_instances

    def describe_db_instances(self, **kwargs: Any) -> RawAWSResponse:
        """Gets AWS RDS db instances

        Returns:
            AWS response for describe_db_instances
        """
        return self.client.describe_db_instances(**kwargs)

    def describe_all_event_subscriptions(self) -> List[Dict[str, Any]]:
        """Gets all AWS RDS event subscriptions

        Returns:
            List of AWS RDS event subscriptions
        """
        response = self.describe_event_subscriptions()
        event_subscriptions = response.get("EventSubscriptionsList", [])
        while "Marker" in response:
            response = self.describe_event_subscriptions(Marker=response.get("Marker"))
            event_subscriptions += response.get("EventSubscriptionsList")
        return event_subscriptions

    def describe_event_subscriptions(self, **kwargs: Any) -> RawAWSResponse:
        """Gets AWS RDS event subscriptions

        [boto3 - describe_event_subscriptions](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.describe_event_subscriptions
        )

        Returns:
            AWS response for describe_event_subscriptions
        """
        return self.client.describe_event_subscriptions(**kwargs)

    def create_event_subscription(
        self,
        subscription_name: str,
        sns_topic_arn: str,
        source_type: str,
        event_categories: List[str],
        **kwargs: Any,
    ) -> RawAWSResponse:
        """Create AWS RDS event subscriptions

        [boto3 - create_event_subscription](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_event_subscription
        )

        Arguments:
            subscription_name -- AWS RDS event subscription name
            sns_topic_arn -- AWS ARN for the sns topic
            source_type -- AWS RDS event subscription source type
            event_categories -- AWS RDS event categories

        Returns:
            AWS response for create_event_subscription
        """
        return self.client.create_event_subscription(
            SubscriptionName=subscription_name,
            SnsTopicArn=sns_topic_arn,
            SourceType=source_type,
            EventCategories=event_categories,
            **kwargs,
        )

    def modify_event_subscription(self, subscription_name: str, **kwargs: Any) -> RawAWSResponse:
        """Modifies an AWS RDS event subscription

        [boto3 - modify_event_subscription](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.modify_event_subscription
        )

        Arguments:
            subscription_name -- AWS RDS event subscription name

        Returns:
            AWs response for modify_event_subscription
        """
        return self.client.modify_event_subscription(SubscriptionName=subscription_name, **kwargs)

    def delete_event_subscription(self, subscription_name: str) -> RawAWSResponse:
        """
        Deletes an existing AWS RDS Event subscription

        [boto3 - delete_event_subscription](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.delete_event_subscription
        )

        Args:
            subscription_name: AWS RDS Subscription name

        Returns:
            Raw AWS Response
        """
        return self.client.delete_event_subscription(SubscriptionName=subscription_name)

    def restore_db_cluster_from_snapshot(
        self,
        db_cluster_id: str,
        db_snapshot_arn: str,
        db_engine: str,
        db_engine_version: str,
        db_cluster_parameter_group_name: str,
        db_subnet_group_name: str,
        vpc_security_group_ids: List[str],
        deletion_protection: Optional[bool] = True,
        enable_iam_auth: Optional[bool] = True,
        pubicly_accessible: Optional[bool] = False,
        **kwargs: Any,
    ) -> RawAWSResponse:
        """
        Restores an RDS instance from an existing RDS Snapshot

        [boto3 - restore_db_cluster_from_snapshot](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.restore_db_cluster_from_snapshot
        )

        Arguments:
            db_instance_id -- AWS RDS db instance id
            db_snapshot_arn -- AWS ARN for the RDS Snapshot
            db_instance_class -- AWS RDS instance class
            db_engine -- AWS RDS Engine
            db_parameter_group_name -- AWS RDS DB Instance parameter group name
            db_option_group_name -- AWS RDS DB Instance option group name
            db_subnet_group_name -- AWS RDS DB subnet group name
            vpc_security_group_ids -- AWS VPC Security Group IDs

        Keyword Arguments:
            deletion_protection -- Toggles deletion protection on AWS RDS Instances
                (default: {True - enabled})
            enable_iam_auth -- Enables IAM authentication for AWS RDS instances
                (default: {True - enabled})
            pubicly_accessible -- Toggles publicly accessible on the RDS Instances
                (default: {False - disabled})

        Returns:
            AWS response for restore_db_instance_from_db_snapshot
        """
        return self.client.restore_db_cluster_from_snapshot(
            DBClusterIdentifier=db_cluster_id,
            SnapshotIdentifier=db_snapshot_arn,
            Engine=db_engine,
            EngineVersion=db_engine_version,
            DBSubnetGroupName=db_subnet_group_name,
            VpcSecurityGroupIds=vpc_security_group_ids,
            DBClusterParameterGroupName=db_cluster_parameter_group_name,
            DeletionProtection=deletion_protection,
            EnableIAMDatabaseAuthentication=enable_iam_auth,
            PubliclyAccessible=pubicly_accessible,
            **kwargs,
        )

    def create_db_instance(
        self,
        db_instance_id: str,
        db_cluster_id: str,
        db_instance_class: str,
        db_instance_parameter_group_name: str,
        db_instance_option_group_name: str,
        db_engine: str,
        db_subnet_group_name: str,
        enable_performance_insights: Optional[bool] = False,
        performance_insights_kms_key_id: Optional[str] = None,
        auto_minor_version_upgrade: Optional[bool] = True,
        **kwargs: Any,
    ) -> RawAWSResponse:
        """Creates an AWS RDS Instance (currently optimized for aurora clusters)

        [boto3 - create_db_instance](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_instance
        )

        Arguments:
            db_instance_id -- AWS RDS Instance ID that will be used when creating the instance
            db_cluster_id -- AWS RDS Cluster ID that the instance will be created in
            db_instance_class -- AWS RDS DB instance class
            db_instance_option_group_name -- AWS RDS DB Option Group name for the instance
            db_engine -- AWS RDS DB Engine
            db_subnet_group_name -- AWS RDS Subnet Group Name

        Keyword Arguments:
            enable_performance_insights -- Enabled performance insights on a given instance.
                If this is set to True, also include performance_insights_kms_key_id
                (default: {False})
            performance_insights_kms_key_id -- AWS KMS Key ID for performance insights data.
                This should be provided if enable_performance_insights is set to True.
                (default: {None - It will use default KMS key for the account})
            auto_minor_version_upgrade -- Toogles auto update of DB minor version updates
                (default: {True})
        """
        if enable_performance_insights and performance_insights_kms_key_id:
            kwargs["PerformanceInsightsKMSKeyId"] = performance_insights_kms_key_id

        return self.client.create_db_instance(
            DBInstanceIdentifier=db_instance_id,
            DBInstanceClass=db_instance_class,
            Engine=db_engine,
            DBSubnetGroupName=db_subnet_group_name,
            DBClusterIdentifier=db_cluster_id,
            DBParameterGroupName=db_instance_parameter_group_name,
            AutoMinorVersionUpgrade=auto_minor_version_upgrade,
            EnablePerformanceInsights=enable_performance_insights,
            OptionGroupName=db_instance_option_group_name,
            **kwargs,
        )

    def create_db_cluster_endpoint(
        self,
        db_cluster_id: str,
        custom_endpoint_id: str,
        endpoint_type: str,
        custom_endpoint_members: List[str],
        **kwargs: Any,
    ) -> RawAWSResponse:
        """Creates an AWS RDS Cluster Custom Endpoint

        [boto3 - create_db_cluster_endpoint](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_cluster_endpoint
        )

        Arguments:
            db_cluster_id -- AWS RDS Cluster ID to add the custom endpoint to
            custom_endpoint_id -- AWS RDS Cluster Endpoint ID to create
            endpoint_type -- AWS RDS Endpoint Type (e.g. READER, WRITER, ANY)
            custom_endpoint_members -- AWS RDS Instances to assign to the endpoint

        Returns:
            AWS response for create_db_cluster_endpoint
        """
        return self.client.create_db_cluster_endpoint(
            DBClusterIdentifier=db_cluster_id,
            DBClusterEndpointIdentifier=custom_endpoint_id,
            EndpointType=endpoint_type,
            StaticMembers=custom_endpoint_members,
            **kwargs,
        )

    def modify_db_instance_certificate(
        self, db_instance_id: str, certificate_id: str, certificate_rotation_restart: bool
    ) -> RawAWSResponse:
        """
        Modifies a RDS Instance CA certificate given a valid CA certificate identifier

        Args:
            db_instance_id: AWS RDS DB instance id
            certificate_id: AWS RDS certificate id
            certificate_rotation_restart: True if RDS Instance should restart for update.
                False if instance will require restart for certificate update

        Returns:
            AWS Response for modify_db_instance
        """
        return self.client.modify_db_instance(
            db_instance_id,
            CACertificateIdentifier=certificate_id,
            CertificateRotationRestart=certificate_rotation_restart,
            ApplyImmediately=True,
        )

    def reboot_db_instance(
        self, db_instance_id: str, force_failover: bool = False
    ) -> RawAWSResponse:
        """
        Reboots an existing AWS RDS db instance

        [boto3 - reboot_db_instance](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.reboot_db_instance
        )

        Args:
            db_instance_id: AWS RDS DB instance id
            force_failover: True if the RDS instance should force fail over. Default false.

        Returns:
            AWS Response for reboot_db_instance
        """
        return self.client.reboot_db_instance(
            DBInstanceIdentifier=db_instance_id, ForceFailover=force_failover
        )

    def tag_resource(self, resource_arn: str, tags: List[Dict[str, str]]) -> RawAWSResponse:
        """Applys tags to an existing RDS resource

        [boto3 - add_tags_to_resource](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.add_tags_to_resource)

        Arguments:
            resource_arn -- AWS ARN of the RDS resource
            tags -- Tags to apply to AWS resources (`[{"Key": "TagKey", "Value", "TagValue"}]`)

        Returns:
            AWS Response for add_tags_to_resource
        """
        return self.client.add_tags_to_resource(ResourceName=resource_arn, Tags=tags)

    def generate_db_auth_token(
        self, host: str, port: int, user: str, aws_region: Optional[str] = None
    ) -> str:
        """
        Generates an auth token used to connect to a db with IAM credentials.

        [boto3 - generate_db_auth_token](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.generate_db_auth_token)

        Arguments
            host (str) -- The hostname of the database to connect to.
            port (int) -- The port number the database is listening on.
            user (str) -- The username to log in as.
            aws_region (str) -- The region the database is in. If None, the client region will be
            used.

        Returns
            A pre-signed url which can be used as an auth token.
        """
        return self.client.generate_db_auth_token(host, port, user, aws_region)
