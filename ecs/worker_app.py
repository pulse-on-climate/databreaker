import os
import logging
import sys
from ecs.converter import convert_netcdf_to_zarr

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def main():
    netcdf_file = os.environ.get('INPUT_FILE')
    zarr_store = os.environ.get('DEST_BUCKET')
    config = os.environ.get('DATASET_CONFIG', '/app/config/dataset_config.json')

    if not netcdf_file:
        print("INPUT_FILE environment variable is not set")
        sys.exit(1)

    print(f"Processing file: {netcdf_file}")
    logger.info(f"Processing file: {netcdf_file}")

    try:
        result = convert_netcdf_to_zarr(
            netcdf_file=netcdf_file,
            zarr_store=f"s3://{zarr_store}",
        )
        logger.info(f"Successfully processed {netcdf_file}")
    except Exception as e:
        logger.error(f"Failed to process {netcdf_file}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 