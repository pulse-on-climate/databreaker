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
from constructs import Construct

class ConversionStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Get VPC
        vpc = ec2.Vpc.from_lookup(self, "VPC", is_default=True)

        # Try to get existing security group, create if it doesn't exist
        try:
            security_group = ec2.SecurityGroup.from_lookup_by_name(
                self, "ConverterSecurityGroup",
                vpc=vpc,
                security_group_name=f"{id.lower()}-converter-sg"
            )
        except:
            security_group = ec2.SecurityGroup(
                self, "ConverterSecurityGroup",
                vpc=vpc,
                description="Security group for converter ECS tasks",
                security_group_name=f"{id.lower()}-converter-sg",
                allow_all_outbound=True
            )

        # Check if bucket exists first
        bucket_name = f"{id.lower()}-source"

        try:
            # Try to get existing bucket
            source_bucket = s3.Bucket.from_bucket_name(
                self, "SourceBucket",
                bucket_name=bucket_name
            )
            print(f"Using existing bucket: {bucket_name}")
        except Exception as e:
            print(f"Creating new bucket: {bucket_name}")
            # Create new bucket if it doesn't exist
            source_bucket = s3.Bucket(
                self, "SourceBucket",
                bucket_name=bucket_name,
                removal_policy=RemovalPolicy.RETAIN,
                auto_delete_objects=False,
                versioned=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL
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
                "SUBNET_IDS": ",".join([subnet.subnet_id for subnet in vpc.private_subnets]) if vpc.private_subnets else vpc.public_subnets[0].subnet_id,
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

        # Create or get existing ECR Repository
        try:
            repository = ecr.Repository.from_repository_name(
                self, "ConversionRepo",
                repository_name="databreaker-converter"
            )
        except:
            repository = ecr.Repository(
                self, "ConversionRepo",
                repository_name="databreaker-converter",
                removal_policy=RemovalPolicy.RETAIN  # Don't delete repo on stack deletion
            )

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self, "ConversionCluster",
            vpc=vpc,
            cluster_name=f"{id.lower()}-conversion-cluster"
        )

        # Try to get existing role, create if it doesn't exist
        # Create a new role each time with unique name
        task_role = iam.Role(
            self, "ECSTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"{id.lower()}-ecs-task-role-{Stack.of(self).account}",  # Make name unique
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                )
            ]
        )

        # Add S3 permissions if needed
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                resources=[
                    source_bucket.bucket_arn,
                    f"{source_bucket.bucket_arn}/*"
                ]
            )
        )
        
        # Create Task Definition
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
                resources=["*"],  # Scope this down to specific task definition if needed
                conditions={
                    "StringEquals": {
                        "ecs:cluster": cluster.cluster_arn
                    }
                }
            )
        )
        
        # Grant Lambda permission to pass task role
        lambda_fn.add_to_role_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[
                    task_role.role_arn,
                    task_definition.execution_role.role_arn  # Add execution role
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