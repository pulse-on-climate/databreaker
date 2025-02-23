import s3fs.errors
import xarray as xr
import re
import pandas as pd
import zarr
import os 
import re
import s3fs
import io 
import fsspec
import numpy as np
import blake3
import struct
import logging

import zarr.errors

def calculate_spatial_hash(lat: float, lon: float, sst: float, err: float, 
                         ice: float, anom: float) -> str:
    """Calculate BLAKE3 hash for a specific lat/lon point and its associated values"""
    try:
        # Replace NaN values with a sentinel value (-999.0)
        sst_val = -999.0 if np.isnan(sst) else sst
        err_val = -999.0 if np.isnan(err) else err
        ice_val = -999.0 if np.isnan(ice) else ice
        anom_val = -999.0 if np.isnan(anom) else anom
        
        value_bytes = struct.pack('6f', lat, lon, sst_val, err_val, ice_val, anom_val)
        return blake3.blake3(value_bytes).hexdigest()
    except Exception as e:
        print(f"Error calculating hash: {str(e)}")
        raise

def calculate_dataset_hashes(ds: xr.Dataset) -> xr.DataArray:
    """Calculate spatial hashes for entire dataset"""
    # Initialize output array
    hash_array = np.empty((len(ds.lat), len(ds.lon)), dtype='U64')
    
    # Get variables as 2D arrays
    sst = ds.sst.squeeze().values  # Squeeze to remove any single dimensions
    err = ds.err.squeeze().values
    ice = ds.ice.squeeze().values
    anom = ds.anom.squeeze().values
    
    # Get lat/lon values
    lats = ds.lat.values
    lons = ds.lon.values
    
    # Process each point
    for i in range(len(lats)):
        for j in range(len(lons)):
            try:
                # Access individual scalar values
                lat_val = float(lats[i])
                lon_val = float(lons[j])
                sst_val = float(sst[i,j])
                err_val = float(err[i,j])
                ice_val = float(ice[i,j])
                anom_val = float(anom[i,j])
                
                hash_value = calculate_spatial_hash(
                    lat_val, lon_val, sst_val, err_val, ice_val, anom_val
                )
                hash_array[i,j] = hash_value
            except Exception as e:
                print(f"Error processing point ({i},{j}): {str(e)}")
                # Set a default hash for error cases
                hash_array[i,j] = calculate_spatial_hash(
                    float(lats[i]), float(lons[j]), 
                    -999.0, -999.0, -999.0, -999.0
                )

    # Create DataArray with same coordinates as input
    return xr.DataArray(
        data=hash_array,
        dims=['lat', 'lon'],
        coords={
            'lat': ds.lat,
            'lon': ds.lon
        },
        name='spatial_hash'
    )

def convert_netcdf_to_zarr(netcdf_file, zarr_store):
    """
    Convert a NetCDF file to a Zarr store, including spatial hashes.
    """
    # Initialize the S3 file system with proper credentials
    s3 = s3fs.S3FileSystem(anon=True)
    
    # Open the NetCDF file
    with s3.open(netcdf_file, mode="rb") as f:
        data = f.read()

    # Bytes to file, then open the file as a dataset
    ds = xr.open_dataset(io.BytesIO(data), engine="h5netcdf").load()
    
    # Calculate spatial hashes
    print("Calculating spatial hashes...")
    spatial_hashes = calculate_dataset_hashes(ds)
    
    # Add spatial hashes to the dataset
    ds['spatial_hash'] = spatial_hashes
    
    # Always update the time coordinate
    try:
        new_time = extract_date_from_filename(netcdf_file).replace(hour=12, minute=0, second=0)
        print(f"Set time dimension to: {new_time}")
    except Exception as e:
         print(f"Error setting time dimension: {e}")
         raise

    # Set up filesystem and store
    fs = fsspec.filesystem("s3", asynchronous=False)
    zarr_store_path = zarr_store.replace("s3://", "")
    store = zarr.storage.FsspecStore(fs=fs, read_only=False, path=zarr_store_path)

    try:
        # Try to open existing dataset
        existing_ds = xr.open_zarr(store, consolidated=True)
        existing_times = pd.to_datetime(existing_ds["time"].values)

        if new_time in existing_times:
            # Overwrite: Remove the existing time slice matching new_time
            updated_ds = existing_ds.drop_sel(time=new_time)
            # Concatenate the updated dataset with the new day's data along "time"
            final_ds = xr.concat([updated_ds, ds], dim="time").sortby("time")
            final_ds.to_zarr(store, mode="w")
            zarr.consolidate_metadata(store)
            print(f"Overwrote existing time slice {new_time} in Zarr store at {zarr_store}")
        else:
            # Append the new day's data to the existing Zarr store
            ds.to_zarr(store, mode="a", append_dim="time")
            print(f"Appended new date {new_time} from {netcdf_file} to existing Zarr store at {zarr_store}")
    except (FileNotFoundError, zarr.errors.ContainsArrayAndGroupError):
        # If the store doesn't exist or we can't access it, create a new one
        ds.to_zarr(store, mode="w")
        zarr.consolidate_metadata(store)
        print(f"Created new Zarr store at {zarr_store} from {netcdf_file}")

    print(f"Successfully processed {zarr_store}")

def extract_date_from_filename(filepath):
    """
    Extract a date from the filename that follows the pattern '.YYYYMMDD.'.
    For example, "oisst-avhrr-v02r01.20250101.nc" returns a Timestamp for 2025-01-01.
    """
    filename = filepath.split("/")[-1]
    print(filename)
    m = re.search(r'\.(\d{8})\.', filename)
    if m:
        filename = filepath
    m = re.search(r'\.(\d{8})\.', filename)
    if m:
         date_str = m.group(1)
         return pd.to_datetime(date_str, format="%Y%m%d")
    raise ValueError("Filename does not contain a valid date in the format '.YYYYMMDD.'") 
