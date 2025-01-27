import xarray as xr
import s3fs
import os
import json
import tempfile
from typing import Dict, Any

def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from environment or file"""
    # Load dataset-specific config if specified
    dataset_config = {}
    dataset_config_path = os.environ.get("DATASET_CONFIG")
    if dataset_config_path and os.path.exists(dataset_config_path):
        with open(dataset_config_path) as f:
            dataset_config = json.load(f)
    
    if config_path and os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)
    
    # Default to environment variables
    env_config = {
        "s3": {
            "endpoint_url": os.environ.get("AWS_ENDPOINT_URL"),
            "access_key_id": os.environ.get("AWS_ACCESS_KEY_ID"),
            "secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "region": os.environ.get("AWS_DEFAULT_REGION")
        },
        "storage": {
            "source_bucket": os.environ.get("SOURCE_BUCKET"),
            "dest_bucket": os.environ.get("DEST_BUCKET"),
            "incoming_prefix": os.environ.get("INCOMING_PREFIX", "incoming/")
        }
    }
    
    # Merge configs, with dataset config taking precedence
    return {**env_config, **dataset_config}

def convert_netcdf_to_zarr(
    source_path: str, 
    dest_path: str, 
    config: Dict[str, Any] = None
) -> Dict[str, str]:
    """
    Convert a single NetCDF file to Zarr format
    
    Args:
        source_path: S3 path to source NetCDF file (s3://bucket/key)
        dest_path: S3 path for output Zarr store (s3://bucket/key)
        config: Configuration dictionary (optional)
    """
    # Load config
    if config is None:
        config = load_config()
    
    # Initialize S3 filesystem
    s3 = s3fs.S3FileSystem(
        endpoint_url=config["s3"]["endpoint_url"],
        key=config["s3"]["access_key_id"],
        secret=config["s3"]["secret_access_key"]
    )
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download source file
        print(f"Downloading {source_path}...")
        nc_file = os.path.join(tmpdir, 'input.nc')
        s3.get(source_path.replace('s3://', ''), nc_file)
        
        # Create local zarr store
        zarr_path = os.path.join(tmpdir, 'output.zarr')
        
        # Open and convert
        print(f"Converting to Zarr...")
        with xr.open_dataset(nc_file, engine='netcdf4') as ds:
            print(f"Dataset loaded: {ds}")
            
            # Convert to Zarr with configured chunks and compression
            encoding = {}
            for var in ds.data_vars:
                encoding[var] = {
                    'chunks': config["conversion"]["variables"][var]["chunks"],
                    'compressor': config["conversion"]["variables"][var]["compressor"]
                }
            
            ds.to_zarr(
                store=zarr_path,
                mode='w',
                encoding=encoding
            )
            print("Dataset written to Zarr store")
            
            # Upload to S3
            print(f"Uploading to {dest_path}...")
            s3.put(zarr_path, dest_path.replace('s3://', ''), recursive=True)
    
    return {
        "source": source_path,
        "destination": dest_path,
        "config": config
    } 