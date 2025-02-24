import boto3, json

def handler(event, context):
    print("Event received:", json.dumps(event))
    
    try:
        # Retrieve additional configuration from the custom resource properties.
        config = event['ResourceProperties'].get('StackConfig', {})
        print("Loaded stack configuration:", json.dumps(config))
        
        # Get bucket name and destination Lambda ARN from the custom resource properties.
        bucket = event['ResourceProperties']['BucketName']
        dest_lambda = event['ResourceProperties']['LambdaArn']
        s3_client = boto3.client('s3')
     
        # Retrieve current notification configuration.
        current_cfg = s3_client.get_bucket_notification_configuration(Bucket=bucket)
     
        # Check if a Lambda notification rule with suffix ".nc" already exists.
        found = False
        for config_item in current_cfg.get('LambdaFunctionConfigurations', []):
            rules = config_item.get('Filter', {}).get('Key', {}).get('FilterRules', [])
            for rule in rules:
                if rule.get('Name') == 'suffix' and rule.get('Value') == '.nc':
                    found = True
                    break
            if found:
                break
     
        if not found:
            # If not found, create a new rule.
            new_rule = {
                "Id": "CDKNotification",
                "LambdaFunctionArn": dest_lambda,
                "Events": ["s3:ObjectCreated:*"],
                "Filter": {
                    "Key": {
                        "FilterRules": [
                            {"Name": "suffix", "Value": ".nc"}
                        ]
                    }
                }
            }
     
            # Append the new rule to any existing rules.
            new_cfg = current_cfg.copy()
            new_cfg.setdefault("LambdaFunctionConfigurations", [])
            new_cfg["LambdaFunctionConfigurations"].append(new_rule)
     
            # Update the bucket notification configuration.
            s3_client.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration=new_cfg
            )
        # Signal success to CloudFormation.
        #cfnresponse.send(event, context, cfnresponse.SUCCESS, {"Message": "Success"})
    except Exception as e:
        print("Error:", str(e))
        # Signal failure to CloudFormation.
        #cfnresponse.send(event, context, cfnresponse.FAILED, {"Message": str(e)})
