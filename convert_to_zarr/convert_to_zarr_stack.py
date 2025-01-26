# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from ast import Lambda
import json

from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_logs as logs,
    aws_iam as iam,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_s3_notifications as s3notify,
    aws_kms as kms,
    aws_lambda as _lambda,
    aws_elasticloadbalancingv2 as elb,
    aws_servicediscovery as sd,
    Stack, 
    Duration, 
    RemovalPolicy,
    CfnOutput
)
from aws_cdk.aws_ecr_assets import DockerImageAsset
from constructs import Construct
from convert_to_zarr.lambda_constructs import (
    NetCDFEventDispatcher,
    NetCDFProducer,
    NetCDFConsumer
)


class ConvertToZarrStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load configuration
        with open('config/buckets.json', 'r') as f:
            config = json.load(f)

        # Source bucket - create or import
        if config['source_bucket']['create']:
            source_bucket = s3.Bucket(
                self,
                "SourceDataBucket",
                bucket_name=config['source_bucket']['name'],
                removal_policy=RemovalPolicy.RETAIN
            )
        else:
            source_bucket = s3.Bucket.from_bucket_name(
                self, 
                "SourceDataBucket",
                config['source_bucket']['name']
            )

        # Destination bucket - create or import
        if config['destination_bucket']['create']:
            destination_bucket = s3.Bucket(
                self, 
                "ZarrDestinationBucket",
                bucket_name=config['destination_bucket']['name'],
                removal_policy=RemovalPolicy.RETAIN
            )
        else:
            destination_bucket = s3.Bucket.from_bucket_name(
                self,
                "ZarrDestinationBucket",
                config['destination_bucket']['name']
            )

        # SQS Queue
        queue = sqs.Queue(self, "NetCDFProcessingQueue")

        # Make buckets and queue accessible to other stacks
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.queue = queue

        # Output the bucket names
        CfnOutput(self, "SourceDataBucketName", value=source_bucket.bucket_name)
        CfnOutput(self, "ZarrDestinationBucketName", value=destination_bucket.bucket_name)

        # VPC networking

        vpc = ec2.Vpc(
            self,
            "zarr-conversion-vpc",
            max_azs=1,
            gateway_endpoints={"s3": ec2.GatewayVpcEndpointOptions(service=ec2.GatewayVpcEndpointAwsService.S3)}
        )

        public_subnets = vpc.public_subnets
        private_subnets = vpc.private_subnets

        # Dask Cluster setup

        dask_asset = DockerImageAsset(
            self, "dask", directory="./docker", file="Dockerfile"
        )

        s_logs = logs.LogGroup(
            self, 'Dask-Scheduler-logs',
            log_group_name='Scheduler-logs',
            removal_policy=RemovalPolicy.DESTROY)

        w_logs = logs.LogGroup(
            self, 'Dask-Worker-logs',
            log_group_name='Worker-logs',
            removal_policy=RemovalPolicy.DESTROY)

        nRole = iam.Role(self, 'ECSExecutionRole', assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'))

        nPolicy = iam.Policy(self, "ECSExecutionPolicy", policy_name="ECSExecutionPolicy")
        nPolicy.add_statements(iam.PolicyStatement(
            actions=[
                'ecr:BatchCheckLayerAvailability',
                'ecr:GetDownloadUrlForLayer',
                'ecr:BatchGetImage',
                'ecr:GetAuthorizationToken'],
            # resources=[f'arn:aws:ecr:{self.region}:{self.account}:repository/*'])
            resources=[dask_asset.repository.repository_arn])
        )

        nPolicy.add_statements(iam.PolicyStatement(
            actions=[
                'logs:CreateLogStream',
                'logs:PutLogEvents'],
            resources=[
                s_logs.log_group_arn,
                w_logs.log_group_arn])
        )
        nPolicy.add_statements(iam.PolicyStatement(
            actions=[
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            resources=[
                source_bucket.bucket_arn,
                f"{source_bucket.bucket_arn}/*",
                destination_bucket.bucket_arn,
                f"{destination_bucket.bucket_arn}/*"
            ]
        ))
        nPolicy.add_statements(iam.PolicyStatement(
            actions=[
                "sqs:ReceiveMessage",
                "sqs:DeleteMessage",
                "sqs:GetQueueAttributes"
            ],
            resources=[queue.queue_arn]
        ))
        nPolicy.attach_to_role(nRole)

        cluster = ecs.Cluster(
            self, 'Dask-Cluster',
            vpc=vpc,
            container_insights=True,
            cluster_name='Dask-Cluster')

        nspace = cluster.add_default_cloud_map_namespace(  # noqa: F841
            name='local-dask',
            type=sd.NamespaceType.DNS_PRIVATE, vpc=vpc)

        # Dask Scheduler

        schedulerTask = ecs.TaskDefinition(
            self, 'taskDefinitionScheduler',
            compatibility=ecs.Compatibility.FARGATE,
            cpu='8192', memory_mib='16384',
            network_mode=ecs.NetworkMode.AWS_VPC,
            placement_constraints=None, execution_role=nRole,
            family='Dask-Scheduler', task_role=nRole
        )

        schedulerTask.add_container(
            'DaskSchedulerImage', image=ecs.ContainerImage.from_docker_image_asset(dask_asset),
            command=['dask', 'scheduler'], cpu=8192, essential=True,
            logging=ecs.LogDriver.aws_logs(stream_prefix='ecs', log_group=s_logs),
            memory_limit_mib=16384, memory_reservation_mib=16384)

        # Dask Worker

        workerTask = ecs.TaskDefinition(
            self, 'taskDefinitionWorker',
            compatibility=ecs.Compatibility.FARGATE,
            cpu='8192', memory_mib='16384',
            network_mode=ecs.NetworkMode.AWS_VPC,
            placement_constraints=None, execution_role=nRole,
            family='Dask-Worker', task_role=nRole)

        workerTask.add_container(
            'DaskWorkerImage', image=ecs.ContainerImage.from_docker_image_asset(dask_asset),
            command=[
                'dask', 'worker', 'dask-scheduler.local-dask:8786',
                '--worker-port', '9000', '--nanny-port', '9001'
            ],
            cpu=8192, essential=True,
            logging=ecs.LogDriver.aws_logs(stream_prefix='ecs', log_group=w_logs),
            memory_limit_mib=16384, memory_reservation_mib=16384)

        # Dask security group

        sg = ec2.SecurityGroup(
            self, 'DaskSecurityGroup',
            vpc=vpc, description='Enable Scheduler ports access',
            security_group_name='DaskSecurityGroup')

        sg.connections.allow_from(
            ec2.Peer.ipv4(public_subnets[0].ipv4_cidr_block),
            ec2.Port.tcp_range(8786, 8789),
            'Inbound dask from public subnet'
        )
        sg.connections.allow_internally(
            ec2.Port.all_tcp(),
            'Inbound from within the SG'
        )

        # Dask Cluster services

        cmap1 = ecs.CloudMapOptions(dns_ttl=Duration.seconds(60), failure_threshold=10, name='Dask-Scheduler')

        schedulerService = ecs.FargateService(  # noqa: F841
            self, 'DaskSchedulerService',
            task_definition=schedulerTask,
            security_groups=[sg],
            cluster=cluster, desired_count=1,
            max_healthy_percent=200, min_healthy_percent=100,
            service_name='Dask-Scheduler', cloud_map_options=cmap1)

        cmap2 = ecs.CloudMapOptions(dns_ttl=Duration.seconds(60), failure_threshold=10, name='Dask-Worker')

        workerService = ecs.FargateService(  # noqa: F841
            self, 'DaskWorkerService',
            task_definition=workerTask,
            security_groups=[sg],
            cluster=cluster, desired_count=1,
            max_healthy_percent=200, min_healthy_percent=100,
            service_name='Dask-Worker', cloud_map_options=cmap2)

        # Network Load balancer in public subnet to forward requests to Dask Scheduler

        nlb = elb.NetworkLoadBalancer(
            self,
            id='dask-dashboard-nlb',
            vpc=vpc,
            internet_facing=True
        )
        listener = nlb.add_listener("listener", port=80)
        nlb_tg = elb.NetworkTargetGroup(
            self,
            id="dask-scheduler-tg",
            target_type=elb.TargetType.IP,
            protocol=elb.Protocol.TCP,
            port=8787,
            vpc=vpc
        )
        listener.add_target_groups("fwd-to-dask-scheduler-tg", nlb_tg)

        # Lambda functions
        self.event_dispatcher = NetCDFEventDispatcher(
            self, 
            "EventDispatcher",
            source_bucket=self.source_bucket,
            destination_bucket=self.destination_bucket,
            queue=self.queue
        )

        self.producer = NetCDFProducer(self, "Producer")
        self.consumer = NetCDFConsumer(self, "Consumer")