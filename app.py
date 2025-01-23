import aws_cdk as cdk
from convert_to_zarr.convert_to_zarr_stack import ConvertToZarrStack

app = cdk.App()
stack = ConvertToZarrStack(app, "ConvertToZarrStack")
app.synth()
