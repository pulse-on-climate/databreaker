import os
import json
import logging
import sys
from ecs.converter import convert_netcdf_to_zarr

# Suppress Botocore HTTP checksum INFO messages
logging.getLogger("botocore.httpchecksum").setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # If command-line arguments are provided, use them.
    # Expected usage: python worker_app.py <INPUT_FILE> <DEST_BUCKET> <CONFIG_PATH>
    if len(sys.argv) >= 4:
        netcdf_file = sys.argv[1]
        dest_bucket = sys.argv[2]
        config_path = sys.argv[3]
        logger.info("Using command-line arguments for configuration.")
    else:
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
        zarr_store = f"{dest_bucket}/{dest_subfolder}"
        print(f"Zarr store: {zarr_store}")
    else:
        zarr_store = f"{dest_bucket}"
        print(f"Zarr store: {zarr_store}")

    if not netcdf_file:
        print("INPUT_FILE is not set or provided")
        sys.exit(1)

    print(f"Processing file: {netcdf_file}")
    logger.info(f"Processing file: {netcdf_file}")

    try:
        result = convert_netcdf_to_zarr(
            netcdf_file=netcdf_file,
            zarr_store=zarr_store,
            suffix=deployment_config.get("defined_suffix", ""),
            conversion_config=deployment_config.get("conversion", {})
        )
        logger.info(f"Successfully processed {netcdf_file}")
    except Exception as e:
        logger.error(f"Failed to process {netcdf_file}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
