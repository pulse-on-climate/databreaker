#!/usr/bin/env python3
import aws_cdk as cdk
from stacks.conversion_stack import ConversionStack
import os

app = cdk.App()

ConversionStack(
    app, "Databreaker",
    env=cdk.Environment(
        account=os.environ["CDK_DEFAULT_ACCOUNT"],
        region=os.environ["CDK_DEFAULT_REGION"]
    )
)

app.synth() 