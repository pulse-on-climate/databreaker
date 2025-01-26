from aws_cdk import (
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_s3_notifications as s3notify,
    Duration,
)
from constructs import Construct

class NetCDFEventDispatcher(Construct):
    def __init__(self, scope: Construct, id: str, source_bucket: s3.IBucket, destination_bucket: s3.IBucket, queue: sqs.Queue):
        super().__init__(scope, id)

        self.lambda_fn = _lambda.Function(
            self,
            "NetCDFEventDispatcher",
            runtime=_lambda.Runtime.PYTHON_3_9,
            handler="handler.main",
            code=_lambda.Code.from_asset("lambda"),
            environment={
                "QUEUE_URL": queue.queue_url,
                "DEST_BUCKET_NAME": destination_bucket.bucket_name,
                "SOURCE_BUCKET_NAME": source_bucket.bucket_name,
            },
            timeout=Duration.seconds(30),
            memory_size=256,
        )

        # Add function URL for testing
        self.lambda_fn.add_function_url(
            auth_type=_lambda.FunctionUrlAuthType.NONE
        )

        # Permissions
        source_bucket.grant_read(self.lambda_fn)
        queue.grant_send_messages(self.lambda_fn)
        
        # S3 Event Notifications
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3notify.LambdaDestination(self.lambda_fn)
        )

class NetCDFProducer(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        self.lambda_fn = _lambda.Function(
            self, 
            'NetCDFProducer',
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler='lambda_producer.producer',
            code=_lambda.Code.from_asset('./lambda')
        )

class NetCDFConsumer(Construct):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        self.lambda_fn = _lambda.Function(
            self, 
            'NetCDFConsumer',
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler='lambda_consumer.consumer',
            code=_lambda.Code.from_asset('./lambda')
        ) 