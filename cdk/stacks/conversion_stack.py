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
    Duration,
    RemovalPolicy,
)
from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId
from constructs import Construct

class ConversionStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Load bucket configuration
        config_path = Path(__file__).parent.parent.parent / "config" / "buckets.json"
        with open(config_path) as f:
            bucket_config = json.load(f)

        # Get VPC (using the default one for simplicity)
        vpc = ec2.Vpc.from_lookup(self, "VPC", is_default=True)

        security_group = ec2.SecurityGroup(
            self, "ConverterSecurityGroup",
            vpc=vpc,
            description="Security group for converter ECS tasks",
            security_group_name=f"{id.lower()}-converter-sg",
            allow_all_outbound=True
        )

        # Get source bucket configuration
        source_bucket_config = bucket_config["source_bucket"]
        bucket_name = source_bucket_config["name"]
        create_bucket = source_bucket_config["create"]

        if not create_bucket:
            # Import the existing bucket.
            # Note: This does not verify that the bucket exists at deploy time.
            source_bucket = s3.Bucket.from_bucket_name(
                self, "SourceBucket",
                bucket_name=bucket_name
            )
            print(f"Using existing external bucket: {bucket_name}")

            # Define the desired bucket policy to allow public read (get object)
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

            # Use a custom resource to update the bucket policy.
            # This call will run on stack creation/update and push the policy.
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
            # Create a new bucket with the desired settings.
            # public_read_access=True will grant public read on objects.
            # We use a less restrictive public access block so that the bucket policy can work.
            source_bucket = s3.Bucket(
                self, "SourceBucket",
                bucket_name=bucket_name,
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
                versioned=True,
                public_read_access=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ACLS
            )
            print(f"Created new bucket with public read access: {bucket_name}")

            # (Optional) Explicitly add a bucket policy for public read access.
            source_bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject"],
                    principals=[iam.AnyPrincipal()],
                    resources=[f"{source_bucket.bucket_arn}/*"],
                    effect=iam.Effect.ALLOW,
                    sid="AllowPublicRead"
                )
            )

        # === DESTINATION BUCKET LOGIC ===
        # Get destination bucket configuration
        dest_bucket_config = bucket_config["destination_bucket"]
        dest_bucket_name = dest_bucket_config["name"]
        create_dest_bucket = dest_bucket_config["create"]

        if not create_dest_bucket:
            dest_bucket = s3.Bucket.from_bucket_name(
                self, "DestBucket",
                bucket_name=dest_bucket_name
            )
            print(f"Using existing external destination bucket: {dest_bucket_name}")

            # Optionally: update public access settings and/or bucket policy using a custom resource
            # (if needed, similar to the source bucket custom resource logic)
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
            print(f"Created new destination bucket with public read access: {dest_bucket_name}")
            dest_bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    actions=["s3:GetObject"],
                    principals=[iam.AnyPrincipal()],
                    resources=[f"{dest_bucket.bucket_arn}/*"],
                    effect=iam.Effect.ALLOW,
                    sid="AllowPublicRead"
                )
            )

        # Create Lambda Function using pre-built zip
        lambda_fn = _lambda.Function(
                self, "ConversionTrigger",
                runtime=_lambda.Runtime.PYTHON_3_9,
                handler="conversion_trigger.lambda_handler",
                code=_lambda.Code.from_asset("../build/lambda.zip"),
                function_name=f"{id.lower()}-conversion-trigger",
                log_retention=logs.RetentionDays.ONE_WEEK,
                environment={
                    "CLUSTER_NAME": f"{id.lower()}-conversion-cluster",
                    "TASK_DEFINITION": f"{id.lower()}-converter",
                    # For subnets, ensure you’re joining the right ones:
                    "SUBNET_IDS": ",".join([subnet.subnet_id for subnet in vpc.private_subnets])
                        if vpc.private_subnets else vpc.public_subnets[0].subnet_id,
                    "SECURITY_GROUP_IDS": security_group.security_group_id
                },
                timeout=Duration.seconds(30),
        )

        # Add S3 notification to Lambda
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_fn),
            s3.NotificationKeyFilter(suffix=".nc")
        )

        # Create or get an existing ECR Repository
        try:
            repository = ecr.Repository.from_repository_name(
                self, "ConversionRepo",
                repository_name="databreaker-converter"
            )
        except Exception:
            repository = ecr.Repository(
                self, "ConversionRepo",
                repository_name="databreaker-converter",
                removal_policy=RemovalPolicy.RETAIN  # Don't delete the repo on stack deletion
            )

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self, "ConversionCluster",
            vpc=vpc,
            cluster_name=f"{id.lower()}-conversion-cluster"
        )

        # === ECS TASK ROLE AND PERMISSIONS ===
        task_role = iam.Role(
            self, "ECSTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"{id.lower()}-ecs-task-role-{Stack.of(self).account}",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonECSTaskExecutionRolePolicy")
            ]
        )

        # Grant permissions on the source bucket:
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    source_bucket.bucket_arn,
                    f"{source_bucket.bucket_arn}/*"
                ]
            )
        )
        
        # Grant permissions on the destination bucket:
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[
                    dest_bucket.bucket_arn,
                    f"{dest_bucket.bucket_arn}/*"
                ]
            )
        )

        # Create ECS Task Definition
        task_definition = ecs.FargateTaskDefinition(
            self, "ConversionTask",
            memory_limit_mib=4096,
            cpu=2048,
            task_role=task_role,
            family=f"{id.lower()}-converter"
        )

        container = task_definition.add_container(
            "converter",
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

        # Grant Lambda permissions to start ECS tasks
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["ecs:RunTask"],
                resources=["*"],  # Consider scoping this down to your task definition if possible
                conditions={
                    "StringEquals": {
                        "ecs:cluster": cluster.cluster_arn
                    }
                }
            )
        )

        # Grant Lambda permission to pass task role and execution role
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[
                    task_role.role_arn,
                    task_definition.execution_role.role_arn
                ]
            )
        )

        # Grant S3 permission to invoke Lambda
        lambda_fn.add_permission(
            "AllowS3Invoke",
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
            action="lambda:InvokeFunction",
            source_arn=source_bucket.bucket_arn
        )
