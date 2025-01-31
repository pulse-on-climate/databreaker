from aws_cdk import (
    Stack,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_ecr as ecr,
    aws_logs as logs,
    Duration,
    RemovalPolicy,
)
from constructs import Construct

class ConversionStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Get VPC
        vpc = ec2.Vpc.from_lookup(self, "VPC", is_default=True)

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

        # Create SQS Queue with unique name
        queue = sqs.Queue(
            self, "ConversionQueue",
            queue_name=f"{id.lower()}-conversion.fifo",
            fifo=True,
            content_based_deduplication=True,
            visibility_timeout=Duration.seconds(900)
        )

        # Create ECS Cluster
        cluster = ecs.Cluster(
            self, "ConversionCluster",
            vpc=vpc,
            cluster_name=f"{id.lower()}-conversion-cluster"
        )

        # Task Role with permissions
        task_role = iam.Role(
            self, "ECSTaskRole",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            role_name=f"{id.lower()}-ecs-task-role"  # Unique role name
        )
        
        task_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonECSTaskExecutionRolePolicy"
            )
        )

        # Add S3 permissions if needed
        task_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                resources=["*"]  # Restrict to specific buckets in production
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
                "QUEUE_URL": queue.queue_url
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

        # Create Lambda Function using pre-built zip
        lambda_fn = _lambda.Function(
            self, "ConversionTrigger",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="conversion_trigger.lambda_handler",
            code=_lambda.Code.from_asset("../build/lambda.zip"),
            environment={
                "QUEUE_URL": queue.queue_url,
            },
            timeout=Duration.seconds(30),
            function_name=f"{id.lower()}-conversion-trigger"
        )

        # Grant permissions
        queue.grant_send_messages(lambda_fn)
        queue.grant_consume_messages(task_role) 