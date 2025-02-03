import xarray as xr
import s3fs
import os
import json
import tempfile
from typing import Dict, Any
from datetime import datetime
import re
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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

def get_zarr_store_path(source_path: str, base_path: str) -> str:
    """Determine Zarr store path from source file path
    
    Example:
        source: s3://noaa-oisst-nc/202501/oisst-avhrr-v02r01.20250108.nc
        returns: s3://noaa-oisst-zarr/2025/01/08.zarr
    """
    parts = source_path.split('/')
    filename = parts[-1]
    for part in parts:
        if len(part) == 6 and part.isdigit():  # YYYYMM format
            year = part[:4]
            month = part[4:]
            # Extract day from filename (assumes format oisst-avhrr-v02r01.YYYYMMDD.nc)
            day = filename.split('.')[-2][-2:]  # Get last 2 digits of date
            return f"{base_path}/{year}/{month}/{day}.zarr"
            
    raise ValueError(f"Could not determine year/month from path: {source_path}")

def initialize_zarr_array(ds: xr.Dataset, zarr_path: str, storage_options: dict) -> None:
    """Initialize a new Zarr array with the correct structure
    
    Args:
        ds: Sample dataset to use for initialization
        zarr_path: Path to Zarr store
        storage_options: S3 storage options
    """
    print(f"Initializing new Zarr array at {zarr_path}")
    
    # Create an empty dataset with the same structure
    init_ds = xr.Dataset(
        data_vars={
            var: (ds[var].dims, [], ds[var].attrs)
            for var in ds.data_vars
        },
        coords={
            coord: (ds[coord].dims, [], ds[coord].attrs)
            for coord in ds.coords
        },
        attrs=ds.attrs
    )
    
    # Write the empty dataset to initialize the store
    init_ds.to_zarr(
        zarr_path,
        mode='w',  # Create new store
        storage_options=storage_options,
        consolidated=True
    )

def extract_date_from_filename(filename: str) -> datetime:
    """Extract date from OISST filename format: oisst-avhrr-v02r01.YYYYMMDD.nc"""
    pattern = r'\.(\d{8})\.'
    match = re.search(pattern, filename)
    if match:
        date_str = match.group(1)
        return datetime.strptime(date_str, '%Y%m%d')
    raise ValueError(f"Could not extract date from filename: {filename}")

def convert_netcdf_to_zarr(source_path, dest, config_path):
    """
    Convert a NetCDF file to a Zarr store.

    Parameters:
      source_path (str): S3 URI of the input NetCDF file.
      dest (str): S3 URI of the destination (i.e. bucket or prefix where output Zarr data will be stored).
      config_path (str): Local path to the configuration file.
    
    Returns:
      result: Some result indicator (e.g. a boolean or a summary of the conversion).
    """
    logger.info(f"Converting file from {source_path} to {dest} using config {config_path}")
    
    # You would load your configuration here
    # For example, config = load_config(config_path)
    
    # Open the source dataset (this might involve downloading the file locally or using an S3FileSystem)
    ds = xr.open_dataset(source_path)
    
    # Process conversion and write to Zarr
    ds.to_zarr(dest, mode="w")
    
    logger.info("Conversion complete.")
    return {"status": "success", "output": dest} 