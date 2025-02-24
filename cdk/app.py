#!/usr/bin/env python3
import json
from pathlib import Path
from aws_cdk import App
from stacks.conversion_stack import ConversionStack
from stacks.notification_stack import NotificationStack
import os

app = App()

# Allow passing a custom config file via context (e.g. via "cdk deploy -c stackConfig=path/to/config.json")
config_file = app.node.try_get_context("stackConfig")
config_file = "../"+config_file
# Convert relative path to an absolute path relative to this file's directory if needed
config_path = Path(config_file)
if not config_path.is_absolute():
    config_path = (Path(__file__).parent / config_file).resolve()

print(f"Loading stack configuration from: {config_path}")

with open(config_path, "r") as f:
    stack_config = json.load(f)

# Create the shared notification stack.
NotificationStack(
    app,
    "SharedNotificationStack",
    bucket_name=stack_config.get("SOURCE_BUCKET")
)

ConversionStack(
    app,
    stack_config["stackName"],
    stack_config=stack_config,
    env={
        'account': os.environ.get("CDK_DEFAULT_ACCOUNT"),
        'region': os.environ.get("CDK_DEFAULT_REGION", stack_config.get("region"))
    }
)

app.synth() 