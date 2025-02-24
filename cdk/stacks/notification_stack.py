from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sns as sns,
    aws_s3_notifications as s3n,
)
from constructs import Construct

class NotificationStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Import the existing source bucket.
        bucket = s3.Bucket.from_bucket_name(self, "SourceBucket", bucket_name)

        # Create an SNS topic for shared notifications.
        self.notification_topic = sns.Topic(
            self, "S3NotificationTopic",
            topic_name="s3-notification-topic"
        )

        # Configure the bucket to send OBJECT_CREATED events (for files ending in ".nc")
        # to the SNS topic.
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SnsDestination(self.notification_topic),
            s3.NotificationKeyFilter(suffix=".nc")
        ) 