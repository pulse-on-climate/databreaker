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

def convert_netcdf_to_zarr(
    source_path: str, 
    base_dest_path: str,
    config: Dict[str, Any] = None
) -> Dict[str, str]:
    """
    Convert a single NetCDF file and append to the main Zarr array
    
    Args:
        source_path: S3 path to source NetCDF file (s3://bucket/YYYYMM/oisst-avhrr-v02r01.YYYYMMDD.nc)
        base_dest_path: Base S3 path for Zarr array (s3://bucket)
    """
    print(f"\n{'='*80}")
    print(f"Starting conversion for file: {source_path}")
    print(f"{'='*80}")
    
    if config is None:
        config = load_config()
    
    # Extract date from filename
    filename = source_path.split('/')[-1]
    file_date = extract_date_from_filename(filename)
    print(f"Extracted date from filename: {file_date.strftime('%Y-%m-%d')}")
    
    # Initialize S3 filesystem
    storage_options = {
        'client_kwargs': {'endpoint_url': config["s3"]["endpoint_url"]},
        'key': config["s3"]["access_key_id"],
        'secret': config["s3"]["secret_access_key"]
    }
    
    # Construct the Zarr group path within the bucket
    zarr_path = f"{base_dest_path}"
    print(f"Using Zarr path: {zarr_path}")
    
    # Initialize S3 filesystem
    s3 = s3fs.S3FileSystem(**storage_options)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download source file
        print(f"Downloading {source_path}...")
        nc_file = os.path.join(tmpdir, 'input.nc')
        s3.get(source_path.replace('s3://', ''), nc_file)
        
        # Read and convert
        print(f"Converting to Zarr...")
        with xr.open_dataset(nc_file, engine='netcdf4') as ds:
            print(f"Original dataset time values: {ds.time.values}")
            
            # Extract date from filename for this specific file
            file_date = extract_date_from_filename(filename)
            ds = ds.assign_coords({
                'time': pd.date_range(
                    start=file_date,
                    periods=1,
                    freq='D',
                    normalize=True  # Start at midnight
                ).map(lambda t: t.replace(hour=12)),  # Set to noon
                'year': ('time', [file_date.year]),
                'month': ('time', [file_date.month]),
                'day': ('time', [file_date.day])
            })
            print(f"Updated dataset time values: {ds.time.values}")
            
            try:
                # Try to open existing store
                existing_ds = xr.open_zarr(
                    zarr_path,
                    storage_options=storage_options
                )
                print(f"Existing Zarr store time values: {existing_ds.time.values}")
                
                # Check if this date already exists
                existing_dates = [pd.Timestamp(t).strftime('%Y-%m-%d') for t in existing_ds.time.values]
                current_date = file_date.strftime('%Y-%m-%d')
                if current_date in existing_dates:
                    print(f"Date {current_date} already exists in store, skipping")
                    return {
                        "source": source_path,
                        "destination": zarr_path,
                        "status": "skipped"
                    }
                
                # Append new data
                print(f"Appending data for {current_date}")
                # Sort by time to maintain chronological order
                # Ensure all variables have consistent time dimension
                combined_ds = {}
                for var in ds.data_vars:
                    # Concatenate each variable separately
                    combined_ds[var] = xr.concat(
                        [existing_ds[var], ds[var]], 
                        dim='time'
                    ).sortby('time')
                
                # Reconstruct dataset with consistent dimensions
                ds = xr.Dataset(
                    data_vars=combined_ds,
                    coords={
                        'time': combined_ds[list(combined_ds.keys())[0]].time,
                        'lat': ds.lat,
                        'lon': ds.lon
                    }
                )
                print(f"Combined dataset time values: {ds.time.values}")
                
                # Verify no duplicate dates
                unique_dates = len(set([pd.Timestamp(t).strftime('%Y-%m-%d') for t in ds.time.values]))
                if unique_dates != len(ds.time):
                    print("Warning: Duplicate dates detected!")
                    # Remove duplicates by taking the latest version for each date
                    ds = ds.groupby('time').last()
                
                # Ensure all variables have the same dimensions
                print("Verifying dimension consistency...")
                time_lengths = {var: len(ds[var].time) for var in ds.data_vars}
                if len(set(time_lengths.values())) > 1:
                    print("Warning: Inconsistent time dimensions detected!")
                    print(f"Time lengths: {time_lengths}")
                    raise ValueError("Inconsistent dimensions across variables")
                
                # Write to existing store
                ds.to_zarr(
                    zarr_path,
                    mode='w',  # Use write mode instead of append to ensure consistency
                    encoding=encoding,  # Keep the encoding from the original creation
                    storage_options=storage_options
                )

            except Exception as e:
                print(f"Creating new Zarr store: {str(e)}")
                # Set encoding for new store
                encoding = {}
                for var in ds.data_vars:
                    encoding[var] = {
                        'chunks': config["conversion"]["variables"][var]["chunks"],
                        'compressors': {
                            'name': config["conversion"]["variables"][var]["compressors"]["name"],
                            'configuration': {
                                'cname': config["conversion"]["variables"][var]["compressors"]["cname"],
                                'clevel': config["conversion"]["variables"][var]["compressors"]["clevel"],
                                'shuffle': 'bitshuffle' if config["conversion"]["variables"][var]["compressors"]["shuffle"] == 2 else 'noshuffle'
                            }
                        }
                    }
                
                # Create new store with encoding
                ds.to_zarr(
                    zarr_path,
                    mode='w',  # Create new store
                    encoding=encoding,
                    storage_options=storage_options
                )
            
    return {
        "source": source_path,
        "destination": zarr_path,
        "config": config
    } 