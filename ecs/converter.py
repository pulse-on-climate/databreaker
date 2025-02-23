import xarray as xr
import re
import pandas as pd
import zarr
import os 
import re
import s3fs
import io 
import fsspec

def convert_netcdf_to_zarr(netcdf_file, zarr_store):
    """
    Convert a NetCDF file to a Zarr store.
    """
    # Initialize the S3 file system.
    s3 = s3fs.S3FileSystem(anon=True)
    # Open the NetCDF file.
    with s3.open(netcdf_file, mode="rb") as f:
        data = f.read()

    # Bytes to file, then open the file as a dataset.
    ds = xr.open_dataset(io.BytesIO(data), engine="h5netcdf").load()
    
    # Always update the time coordinate so that it is set to the file date at 12:00:00.
    try:
        new_time = extract_date_from_filename(netcdf_file).replace(hour=12, minute=0, second=0)
        print(f"Set time dimension to: {new_time}")
    except Exception as e:
         print(f"Error setting time dimension: {e}")
         raise

    zarr_file = zarr_store + "/zarr.json"
    # Convert the NetCDF file to a Zarr store.
    if s3.exists(zarr_file):
         # If the store exists, open the existing dataset and check the time coordinate.
         existing_ds = xr.open_zarr(zarr_store, consolidated=True)
         existing_times = pd.to_datetime(existing_ds["time"].values)

         if new_time in existing_times:
              # Overwrite: Remove the existing time slice matching new_time.
              updated_ds = existing_ds.drop_sel(time=new_time)
              # Concatenate the updated dataset with the new day's data along "time"
              final_ds = xr.concat([updated_ds, ds], dim="time").sortby("time")
              final_ds.to_zarr(zarr_store, mode="w")
              store = zarr.storage.LocalStore(zarr_store)
              zarr.consolidate_metadata(store)
              print(f"Overwrote existing time slice {new_time} in Zarr store at {zarr_store}")
         else:
              # Append the new day's data to the existing Zarr store.
              ds.to_zarr(zarr_store, mode="a", append_dim="time")
              print(f"Appended new date {new_time} from {netcdf_file} to existing Zarr store at {zarr_store}")
    else:
         ds.to_zarr(zarr_store, mode="w")
         fs = fsspec.filesystem("s3", asynchronous=False)
         # remove the s3:// from the path
         zarr_store = zarr_store.replace("s3://", "")
         print(zarr_store)
         store = zarr.storage.FsspecStore(fs=fs,read_only=False, path=zarr_store)
         zarr.consolidate_metadata(store)
         print(f"Created new Zarr store at {zarr_store} from {netcdf_file}")
    print(zarr_store)


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
