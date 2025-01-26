from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_s3_notifications as s3notify,
)
from constructs import Construct

class NetCDFEventDispatcherStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, source_bucket: s3.IBucket, destination_bucket: s3.IBucket, queue: sqs.Queue, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

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
        )

        # Permissions
        source_bucket.grant_read(self.lambda_fn)
        queue.grant_send_messages(self.lambda_fn)
        
        # S3 Event Notifications
        source_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED, 
            s3notify.LambdaDestination(self.lambda_fn)
        )

class NetCDFProducerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_fn = _lambda.Function(
            self, 
            'NetCDFProducer',
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler='lambda_producer.producer',
            code=_lambda.Code.from_asset('./lambda')
        )

class NetCDFConsumerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.lambda_fn = _lambda.Function(
            self, 
            'NetCDFConsumer',
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler='lambda_consumer.consumer',
            code=_lambda.Code.from_asset('./lambda')
        ) 