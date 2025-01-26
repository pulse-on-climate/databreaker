import aws_cdk as cdk
from convert_to_zarr.convert_to_zarr_stack import ConvertToZarrStack
import json

app = cdk.App()

# Load configuration
with open('config/buckets.json', 'r') as f:
    config = json.load(f)

env = cdk.Environment(
    account=app.node.try_get_context('account') or process.env.CDK_DEFAULT_ACCOUNT,
    region=app.node.try_get_context('region') or process.env.CDK_DEFAULT_REGION
)

# Configure synthesizer
synthesizer = cdk.DefaultStackSynthesizer(
    qualifier="hnb659fds",
    file_assets_bucket_name=config['staging_bucket']['name']
)

# Deploy stack
stack = ConvertToZarrStack(app, "ConvertToZarrStack", env=env, synthesizer=synthesizer)

app.synth()