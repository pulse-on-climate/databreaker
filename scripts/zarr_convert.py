import xarray as xr
import re
import pandas as pd
import zarr
import os 
import re
import s3fs
import h5netcdf
import io 
import fsspec

def convert_netcdf_to_zarr(netcdf_file, zarr_file):

    s3 = s3fs.S3FileSystem(anon=True)
    with s3.open(netcdf_file, mode="rb") as f:
        data = f.read()

    ds = xr.open_dataset(io.BytesIO(data), engine="h5netcdf").load()
    # Always update the time coordinate so that it is set to the file date at 12:00:00.
    try:
        new_time = extract_date_from_filename(netcdf_file).replace(hour=12, minute=0, second=0)
        print(f"Set time dimension to: {new_time}")
    except Exception as e:
         print(f"Error setting time dimension: {e}")
         raise

    # Convert the NetCDF file to a Zarr store.
    if os.path.exists(zarr_file):
         # If the store exists, open the existing dataset and check the time coordinate.
         existing_ds = xr.open_zarr(zarr_file, consolidated=True)
         existing_times = pd.to_datetime(existing_ds["time"].values)
         if new_time in existing_times:
              # Overwrite: Remove the existing time slice matching new_time.
              updated_ds = existing_ds.drop_sel(time=new_time)
              # Concatenate the updated dataset with the new day's data along "time"
              final_ds = xr.concat([updated_ds, ds], dim="time").sortby("time")
              final_ds.to_zarr(zarr_file, mode="w")
              store = zarr.storage.LocalStore(zarr_file)
              zarr.consolidate_metadata(store)
              print(f"Overwrote existing time slice {new_time} in Zarr store at {zarr_file}")
         else:
              ds.to_zarr(zarr_file, mode="a", append_dim="time")
              print(f"Appended new date {new_time} from {netcdf_file} to existing Zarr store at {zarr_file}")
    else:
         ds.to_zarr(zarr_file, mode="w")
         fs = fsspec.filesystem("s3", asynchronous=True)
         # remove the s3:// from the path
         zarr_file = zarr_file.replace("s3://", "")
         print(zarr_file)
         store = zarr.storage.FsspecStore(fs=fs,read_only=False, path=zarr_file)
         zarr.consolidate_metadata(store)
         print(f"Created new Zarr store at {zarr_file} from {netcdf_file}")
    print(zarr_file)
    return zarr_file 


def extract_date_from_filename(filepath):
    """
    Extract a date from the filename that follows the pattern '.YYYYMMDD.'.
    For example, "oisst-avhrr-v02r01.20250101.nc" returns a Timestamp for 2025-01-01.
    """
    filename = filepath.split("/")[-1]
    print(filename)
    m = re.search(r'\.(\d{8})\.', filename)
    if m:
        filename = os.path.basename(filepath)
    m = re.search(r'\.(\d{8})\.', filename)
    if m:
         date_str = m.group(1)
         return pd.to_datetime(date_str, format="%Y%m%d")
    raise ValueError("Filename does not contain a valid date in the format '.YYYYMMDD.'") 

def print_avg_sst(zarr_path):
    """
    Open the consolidated Zarr store and for each time slice,
    compute and print the average SST value.
    Assumes the dataset has a variable named 'sst' with spatial dimensions.
    """
    ds = xr.open_zarr(zarr_path, consolidated=True)
    for t in ds.time.values:
        print(f"\nSST grid for time {t}:")
        # Select and extract the SST data for the given time slice.
        sst_grid = ds.sel(time=t)["sst"].values
        print(sst_grid) 

if __name__ == "__main__":
    #convert_netcdf_to_zarr("s3://databreaker-source/oisst-avhrr-v02r01.20250101.nc", "s3://databreaker-source-zarr")
    zgroup = zarr.open("s3://databreaker-source-zarr")       
    print(zgroup.tree())
    print(dict(zgroup["time"].attrs))
    # print each time value in the time dimension
    for time in zgroup["time"]:
         print(time)
