import json
from pathlib import Path

from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_logs as logs,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    RemovalPolicy,
)
from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId
from constructs import Construct

class ConversionStack(Stack):
    def __init__(self, scope: Construct, id: str, stack_config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # === Extract configuration values ===
        source_bucket_name: str = (
            stack_config.get("SOURCE_BUCKET")
            or stack_config.get("sourceBucket")
            or stack_config.get("ecsTask", {}).get("environment", {}).get("SOURCE_BUCKET")
        )
        source_bucket_create: bool = stack_config.get("sourceBucketCreate", False)
        # New configuration properties:
        # - sourceBucketType: "internal" or "external"
        # - sourceBucketSnsArn: optional ARN for external buckets that send SNS notifications.
        source_bucket_type: str = stack_config.get("sourceBucketType", "internal")
        source_bucket_sns_arn: str = stack_config.get("sourceBucketSnsArn", None)

        dest_bucket_name: str = (
            stack_config.get("DEST_BUCKET")
            or stack_config.get("destinationBucket")
            or stack_config.get("ecsTask", {}).get("environment", {}).get("DEST_BUCKET")
        )
        dest_bucket_create: bool = stack_config.get("destinationBucketCreate", False)
        ecs_task_config = stack_config.get("ecsTask", {})
        vpc_id = stack_config.get("vpcId")

        # === VPC and Security Group ===
        if vpc_id:
            vpc = ec2.Vpc.from_lookup(self, "VPC", vpc_id=vpc_id)
        else:
            vpc = ec2.Vpc.from_lookup(self, "VPC", is_default=True)

        security_group = ec2.SecurityGroup(
            self, "ConverterSecurityGroup",
            vpc=vpc,
            description="Security group for converter ECS tasks",
            security_group_name=f"{id.lower()}-converter-sg",
            allow_all_outbound=True
        )

        # === SOURCE BUCKET CONFIGURATION ===
        if source_bucket_type == "internal":
            if not source_bucket_create:
                # Import an existing internal bucket.
                source_bucket = s3.Bucket.from_bucket_name(
                    self, "SourceBucket",
                    bucket_name=source_bucket_name
                )
                print(f"Using internal bucket: {source_bucket_name}")

                # Update bucket policy to allow public read.
                desired_policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "AllowPublicRead",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": f"{source_bucket.bucket_arn}/*"
                        }
                    ]
                }
                AwsCustomResource(
                    self, "UpdateBucketPolicy",
                    on_create={
                        "service": "S3",
                        "action": "putBucketPolicy",
                        "parameters": {
                            "Bucket": source_bucket.bucket_name,
                            "Policy": json.dumps(desired_policy)
                        },
                        "physical_resource_id": PhysicalResourceId.of(source_bucket.bucket_name)
                    },
                    policy=AwsCustomResourcePolicy.from_statements([
                        iam.PolicyStatement(
                            actions=["s3:PutBucketPolicy"],
                            resources=[source_bucket.bucket_arn]
                        )
                    ])
                )
            else:
                # Create a new internal bucket.
                source_bucket = s3.Bucket(
                    self, "SourceBucket",
                    bucket_name=source_bucket_name,
                    removal_policy=RemovalPolicy.RETAIN,
                    auto_delete_objects=False,
                    versioned=True,
                    public_read_access=True,
                    block_public_access=s3.BlockPublicAccess.BLOCK_ACLS
                )
                print(f"Created new internal bucket: {source_bucket_name}")
                source_bucket.add_to_resource_policy(
                    iam.PolicyStatement(
                        actions=["s3:GetObject"],
                        principals=[iam.AnyPrincipal()],
                        resources=[f"{source_bucket.bucket_arn}/*"],
                        effect=iam.Effect.ALLOW,
                        sid="AllowPublicRead"
                    )
                )
        else:
            # External bucket scenario: import without modifying its policy.
            source_bucket = s3.Bucket.from_bucket_name(
                self, "ExternalSourceBucket",
                bucket_name=source_bucket_name
            )
            print(f"Using external bucket: {source_bucket_name}")

        # === DESTINATION BUCKET CONFIGURATION ===
        if not dest_bucket_create:
            dest_bucket = s3.Bucket.from_bucket_name(
                self, "DestBucket",
                bucket_name=dest_bucket_name
            )
            print(f"Using existing destination bucket: {dest_bucket_name}")
        else:
            dest_bucket = s3.Bucket(
                self, "DestBucket",
                bucket_name=dest_bucket_name,
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
                versioned=True,
                public_read_access=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ACLS
            )
            print(f"Created new destination bucket: {dest_bucket_name}")
            dest_bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject"],
                    principals=[iam.AnyPrincipal()],
                    resources=[f"{dest_bucket.bucket_arn}/*"],
                    effect=iam.Effect.ALLOW,
                    sid="AllowPublicRead"
                )
            )

        # === LAMBDA FUNCTION FOR CONVERSION TRIGGER ===
        lambda_fn = _lambda.Function(
            self, "ConversionTrigger",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="conversion_trigger.lambda_handler",
            code=_lambda.Code.from_asset("../build/lambda.zip"),
            function_name=f"{id.lower()}-conversion-trigger",
            log_retention=logs.RetentionDays.ONE_WEEK,
            environment={
                "CLUSTER_NAME": f"{id.lower()}-cluster",
                "TASK_DEFINITION": f"{id.lower()}-converter",
                "SUBNET_IDS": ",".join([subnet.subnet_id for subnet in vpc.private_subnets])
                    if vpc.private_subnets else vpc.public_subnets[0].subnet_id,
                "SECURITY_GROUP_IDS": security_group.security_group_id,
                "DEST_BUCKET": dest_bucket.bucket_name
            },
            timeout=Duration.seconds(30),
        )

        # === NOTIFICATION CONFIGURATION ===
        if source_bucket_type == "external":
            if source_bucket_sns_arn:
                # For external buckets, if an SNS ARN is provided, subscribe the Lambda to that topic.
                external_topic = sns.Topic.from_topic_arn(
                    self, "ExternalSnsTopic",
                    topic_arn=source_bucket_sns_arn
                )
                external_topic.add_subscription(subscriptions.LambdaSubscription(lambda_fn))
                print(f"Subscribed Lambda {lambda_fn.function_name} to external SNS topic: {source_bucket_sns_arn}.")
            else:
                # For external buckets with no SNS ARN, we assume you'll use polling.
                print(f"No SNS ARN provided for external bucket {source_bucket_name}; polling method will be used.")
        else:
            # For internal buckets, use SNS notifications.
            internal_topic = sns.Topic.from_topic_arn(
                self, "InternalSnsTopic",
                topic_arn=f"arn:aws:sns:{Stack.of(self).region}:{self.account}:s3-internal-notification-topic"
            )
            internal_topic.add_subscription(subscriptions.LambdaSubscription(lambda_fn))
            print(f"Subscribed Lambda {lambda_fn.function_name} to internal SNS notifications.")
            source_bucket.add_event_notification(
                s3.EventType.OBJECT_CREATED,
                s3n.SnsDestination(internal_topic),
                s3.NotificationKeyFilter(suffix=".nc")
            )

        # === POLLING METHOD SETUP ===
        # For external buckets without an SNS ARN, add a polling Lambda that will periodically check for new files.
        polling_lambda = None
        if source_bucket_type == "external" and not source_bucket_sns_arn:
            polling_lambda = _lambda.Function(
                self, "PollingHandler",
                runtime=_lambda.Runtime.PYTHON_3_9,
                handler="polling_handler.lambda_handler",
                code=_lambda.Code.from_asset("../build/polling_handler.zip"),
                function_name=f"{id.lower()}-polling-handler",
                log_retention=logs.RetentionDays.ONE_WEEK,
                environment={
                    "SOURCE_BUCKET": source_bucket.bucket_name,
                    "CLUSTER_NAME": f"{id.lower()}-cluster",
                    "TASK_DEFINITION": f"{id.lower()}-converter",
                    "SUBNET_IDS": ",".join(
                        [subnet.subnet_id for subnet in vpc.private_subnets]
                    ) if vpc.private_subnets else vpc.public_subnets[0].subnet_id,
                    "SECURITY_GROUP_IDS": security_group.security_group_id,
                    "DEST_BUCKET": dest_bucket.bucket_name,
                    "LAST_PROCESSED_PARAM": "/my-app/last_processed",
                    "POLLING_START_TIMESTAMP": "2025-02-23T00:00:00+00:00"
                },
                timeout=Duration.seconds(120),
            )

            polling_rule = events.Rule(
                self, "PollingScheduleRule",
                schedule=events.Schedule.rate(Duration.minutes(15))
            )
            polling_rule.add_target(targets.LambdaFunction(polling_lambda))
            print(f"Polling method enabled for external bucket {source_bucket.bucket_name}.")

            # Grant permissions for SSM parameter access to the polling Lambda
            polling_lambda.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["ssm:GetParameter", "ssm:PutParameter"],
                    resources=[f"arn:aws:ssm:{Stack.of(self).region}:{self.account}:parameter/my-app/last_processed"]
                )
            )

            # Grant polling Lambda permission to run ECS tasks using its task definition.
            # Using a wildcard for revisions, as task definitions include a revision number.
            polling_lambda.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["ecs:RunTask"],
                    resources=[f"arn:aws:ecs:{Stack.of(self).region}:{self.account}:task-definition/{id.lower()}-converter*"]
                )
            )

        # === ECR REPOSITORY SETUP ===
        try:
            repository = ecr.Repository.from_repository_name(
                self, "ConversionRepo",
                repository_name="databreaker-converter"
            )
        except Exception:
            repository = ecr.Repository(
                self, "ConversionRepo",
                repository_name="databreaker-converter",
                removal_policy=RemovalPolicy.RETAIN
            )

        # === ECS CLUSTER CREATION ===
        cluster = ecs.Cluster(
            self, "ConversionCluster",
            vpc=vpc,
            cluster_name=f"{id.lower()}-cluster"
        )

        # === ECS TASK ROLE AND PERMISSIONS ===
        task_role = iam.Role(
            self, "ECSTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"{id.lower()}-role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    source_bucket.bucket_arn,
                    f"{source_bucket.bucket_arn}/*"
                ]
            )
        )
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[
                    dest_bucket.bucket_arn,
                    f"{dest_bucket.bucket_arn}/*"
                ]
            )
        )

        # === ECS TASK DEFINITION ===
        task_definition = ecs.FargateTaskDefinition(
            self, "ConversionTask",
            memory_limit_mib=int(ecs_task_config.get("memory", "4096")),
            cpu=int(ecs_task_config.get("cpu", "2048")),
            task_role=task_role,
            family=f"{id.lower()}-converter"
        )

        container = task_definition.add_container(
            ecs_task_config.get("containerName", "converter"),
            image=ecs.ContainerImage.from_ecr_repository(repository),
            command=["python", "-m", "ecs.worker_app"],
            environment={
                "PYTHONPATH": "/app",
                "SOURCE_BUCKET": source_bucket.bucket_name,
                "DEST_BUCKET": dest_bucket.bucket_name,
                "AWS_DEFAULT_REGION": Stack.of(self).region,
                "DATASET_CONFIG": "/app/config/app_config.json"
            },
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="converter",
                log_group=logs.LogGroup(
                    self, "ConversionLogGroup",
                    log_group_name=f"/ecs/{id.lower()}-converter",
                    retention=logs.RetentionDays.TWO_WEEKS,
                    removal_policy=RemovalPolicy.DESTROY
                )
            )
        )

        # === LAMBDA PERMISSIONS FOR ECS TASK TRIGGERING ===
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=["*"],  # Consider scoping this down if possible.
                conditions={
                    "StringEquals": {
                        "ecs:cluster": cluster.cluster_arn
                    }
                }
            )
        )

        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[
                    task_role.role_arn,
                    task_definition.execution_role.role_arn
                ]
            )
        )

        # Only add S3 invoke permission if we own the bucket (internal)
        if source_bucket_type == "internal":
            lambda_fn.add_permission(
                "AllowS3Invoke",
                principal=iam.ServicePrincipal("s3.amazonaws.com"),
                action="lambda:InvokeFunction",
                source_arn=source_bucket.bucket_arn
            )

        # Grant polling Lambda iam:PassRole permission if defined.
        if polling_lambda is not None:
            polling_lambda.add_to_role_policy(
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    resources=[
                        task_role.role_arn,
                        task_definition.execution_role.role_arn
                    ]
                )
            )
