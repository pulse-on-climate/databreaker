#!/usr/bin/env python3
import sys
import os
# Add the project root to sys.path so that the ecs package can be found.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import tempfile
import shutil
import logging
from ecs.converter import convert_netcdf_to_zarr

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Path to a local sample NetCDF file. Create one or obtain a sample.
    sample_nc = os.path.abspath("/Users/andrew/Documents/projects/databreaker/tests/data/oisst-avhrr-v02r01.20250101.nc")
    if not os.path.exists(sample_nc):
        logger.error("Sample NetCDF file not found at sample_data/test.nc")
        sys.exit(1)

    # For testing, we simulate S3 paths by using the file:// scheme.
    input_file = f"file://{sample_nc}"
    # Create a temporary directory for the Zarr store.
    dest_dir = tempfile.mkdtemp(prefix="zarr_test_")
    dest_store = f"file://{dest_dir}"
    
    # Set the DATASET_CONFIG environment variable to point to your local OISST config.
    config_file = os.path.abspath("config/oisst.json")
    os.environ["DATASET_CONFIG"] = config_file
    # Ensure AWS_DEFAULT_REGION is set (used in load_config and converter).
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    logger.info(f"Testing conversion from {input_file} to {dest_store} using config {config_file}")
    
    # Patch s3fs.S3FileSystem.get so that it handles file:// paths by copying the local file.
    import s3fs
    original_get = s3fs.S3FileSystem.get

    def local_get(self, s3_path, local_path, **kwargs):
        # Remove potential leading slashes (simulate s3 key from file:// path).
        s3_key = s3_path.lstrip("/")
        # If the local file exists, copy it.
        if os.path.exists(s3_key):
            shutil.copy2(s3_key, local_path)
        else:
            original_get(self, s3_path, local_path, **kwargs)

    s3fs.S3FileSystem.get = local_get

    try:
        result = convert_netcdf_to_zarr(
            source_path=input_file,
            base_dest_path=dest_store,
            config=config_file
        )
        logger.info(f"Conversion result: {result}")
        logger.info(f"Zarr store created at: {dest_dir}")
    except Exception as e:
        logger.error(f"Conversion test failed: {e}")
        sys.exit(1)
    finally:
        # Restore the original get method.
        s3fs.S3FileSystem.get = original_get

if __name__ == "__main__":
    main() 