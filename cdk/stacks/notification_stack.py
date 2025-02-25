from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_sns as sns,
    aws_s3_notifications as s3n,
)
from constructs import Construct

class NotificationStack(Stack):
    def __init__(self, scope: Construct, id: str, bucket_name: str, source_bucket_type: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Import the existing source bucket.
        bucket = s3.Bucket.from_bucket_name(self, "SourceBucket", bucket_name)
        
        # Only attach notifications if the bucket is internal.
        if source_bucket_type.lower() == "internal":
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
        else:
            # For external buckets, do not attach any notifications.
            self.notification_topic = None
