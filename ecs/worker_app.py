import os
import json
import logging
import sys
from ecs.converter import convert_netcdf_to_zarr

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    netcdf_file = os.environ.get('INPUT_FILE')
    dest_bucket = os.environ.get('DEST_BUCKET')
    config_path = os.environ.get('DATASET_CONFIG', '/app/config/app_config.json')
    print(f"Loading deployment configuration from: {config_path}")
    try:
        with open(config_path, 'r') as f:
            deployment_config = json.load(f)
    except Exception as e:
        print(f"Failed to load deployment configuration from {config_path}: {e}")
        sys.exit(1)

    # Determine the subfolder (if specified) and build the final S3 path.
    dest_subfolder = deployment_config.get("sub_folder", "").strip("/")
    if dest_subfolder:
        zarr_store = f"s3://{dest_bucket}/{dest_subfolder}"
        print(f"Zarr store: {zarr_store}")
    else:
        zarr_store = f"s3://{dest_bucket}"
        print(f"Zarr store: {zarr_store}")

    if not netcdf_file:
        print("INPUT_FILE environment variable is not set")
        sys.exit(1)

    print(f"Processing file: {netcdf_file}")
    logger.info(f"Processing file: {netcdf_file}")

    try:
        result = convert_netcdf_to_zarr(
            netcdf_file=netcdf_file,
            zarr_store=zarr_store,
        )
        logger.info(f"Successfully processed {netcdf_file}")
    except Exception as e:
        logger.error(f"Failed to process {netcdf_file}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 