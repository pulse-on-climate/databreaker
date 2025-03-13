import s3fs.errors
import xarray as xr
import re
import pandas as pd
import zarr
import s3fs
import io 
import fsspec
import numpy as np
import blake3
import struct
import dask.array as da
import zarr.errors
import logging
import os

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def calculate_spatial_hash(lat: float, lon: float, sst: float, err: float, 
                           ice: float, anom: float) -> str:
    """Calculate BLAKE3 hash for a specific lat/lon point and its associated values."""
    try:
        sst_val = -999.0 if np.isnan(sst) else sst
        err_val = -999.0 if np.isnan(err) else err
        ice_val = -999.0 if np.isnan(ice) else ice
        anom_val = -999.0 if np.isnan(anom) else anom
        value_bytes = struct.pack('6f', lat, lon, sst_val, err_val, ice_val, anom_val)
        return blake3.blake3(value_bytes).hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash: {str(e)}")
        raise

def safe_spatial_hash(lat, lon, sst_val, err_val, ice_val, anom_val):
    try:
        result = calculate_spatial_hash(lat, lon, sst_val, err_val, ice_val, anom_val)
        # Debug: print the result for the first few calls.
        # (You might want to remove or comment this out once confirmed.)
        # print(f"Hash computed for ({lat}, {lon}): {result}")
        return result
    except Exception as e:
        logger.error(f"Error processing (lat,lon)=({lat},{lon}): {e}")
        return calculate_spatial_hash(lat, lon, -999.0, -999.0, -999.0, -999.0)


def calculate_dataset_hashes(ds: xr.Dataset) -> xr.DataArray:
    """
    Calculate spatial hashes for the dataset.
    If 'zlev' exists, compute using only the first level, then expand the result
    to include a singleton 'zlev' dimension so that the output dimensions become (time, zlev, lat, lon).
    """
    logger.debug("Starting spatial hash calculation.")
    if 'zlev' in ds.dims:
        ds_for_hash = ds.isel(zlev=0)
        logger.debug("Using first zlev level for hash calculation.")
    else:
        ds_for_hash = ds
        logger.debug("No 'zlev' dimension found; using dataset directly for hash calculation.")

    # Broadcast 1D lat and lon to a 2D grid.
    lat2d, lon2d = xr.broadcast(ds_for_hash.lat, ds_for_hash.lon)
    # Expand to include time dimension.
    if 'time' in ds_for_hash.dims:
        lat3d = lat2d.expand_dims({'time': ds_for_hash.time}, axis=0)
        lon3d = lon2d.expand_dims({'time': ds_for_hash.time}, axis=0)
    else:
        lat3d, lon3d = lat2d, lon2d

    # Let the output be of object dtype so that each element is a full Python string.
    hash_array = xr.apply_ufunc(
        safe_spatial_hash,
        lat3d, lon3d,
        ds_for_hash.sst, ds_for_hash.err, ds_for_hash.ice, ds_for_hash.anom,
        vectorize=True,
        dask="parallelized",
        output_dtypes=[object]
    )
    hash_array.name = "spatial_hash"
    hash_array = hash_array.assign_coords(time=ds_for_hash.time, lat=ds_for_hash.lat, lon=ds_for_hash.lon)

    # If the original dataset has 'zlev', add it back as a singleton dimension.
    if 'zlev' in ds.dims:
        hash_array = hash_array.expand_dims('zlev', axis=1)
        hash_array = hash_array.assign_coords(zlev=ds.zlev)
        logger.debug("Expanded spatial hash to include singleton 'zlev' dimension.")
    logger.debug("Completed spatial hash calculation.")
    return hash_array

def extract_date_from_filename(filepath, suffix):
    """
    Extract a date from the filename that follows the pattern '.YYYYMMDD.'.
    Removes a defined suffix (e.g., '_preliminary') if present.
    """
    filename = filepath.split("/")[-1]
    logger.debug(f"Original filename: {filename}")
    if suffix and suffix in filename:
        filename = filename.replace(suffix, "")
        logger.debug(f"Removed suffix '{suffix}'. New filename: {filename}")
    m = re.search(r'\.(\d{8})\.', filename)
    if m:
         date_str = m.group(1)
         logger.debug(f"Extracted date string: {date_str}")
         return pd.to_datetime(date_str, format="%Y%m%d")
    raise ValueError(f"Filename does not contain a valid date in the format '.YYYYMMDD.': {filename}")

def load_dataset(netcdf_file, suffix, conversion_config=None):
    """
    Load the NetCDF dataset, add a missing 'zlev' dimension if needed,
    rechunk the dataset (using conversion_config if provided), and extract the new time dimension.
    """
    logger.info(f"Opening dataset from {netcdf_file}")
    ds = xr.open_dataset(netcdf_file, engine="h5netcdf")
    logger.info("Dataset loaded successfully.")
    
    # Add 'zlev' dimension if missing.
    if 'zlev' not in ds.dims:
        ds = ds.expand_dims('zlev')
        ds = ds.assign_coords(zlev=[1])
        logger.info("Added missing 'zlev' dimension with default value 1.")
    else:
        logger.info("'zlev' dimension exists in dataset.")
    
    # Rechunk the dataset using conversion_config if available.
    if conversion_config and "variables" in conversion_config:
        for var, var_conf in conversion_config["variables"].items():
            if var in ds:
                chunks = var_conf.get("chunks", {})
                valid_chunks = {dim: size for dim, size in chunks.items() if dim in ds[var].dims}
                if valid_chunks:
                    logger.info(f"Rechunking variable '{var}' with chunks: {valid_chunks}")
                    ds[var] = ds[var].chunk(valid_chunks)
    else:
        # Use a default chunking if none specified.
        ds = ds.chunk({'time': 1, 'zlev': 1, 'lat': 72, 'lon': 144})
        logger.info("Rechunked dataset with default chunks: {'time': 1, 'zlev': 1, 'lat': 72, 'lon': 144}")
    
    try:
        new_time = extract_date_from_filename(netcdf_file, suffix).replace(hour=12, minute=0, second=0)
        logger.info(f"Extracted new time dimension: {new_time}")
    except Exception as e:
        logger.error(f"Error extracting time dimension: {e}")
        raise
    return ds, new_time

def add_spatial_hashes(ds):
    """
    Compute spatial hashes and add the 'spatial_hash' variable to the dataset.
    """
    logger.info("Calculating spatial hashes lazily...")
    spatial_hashes = calculate_dataset_hashes(ds)
    ds['spatial_hash'] = spatial_hashes
    logger.info("Spatial hashes added to dataset.")
    return ds

def add_verifier_pubkeys(ds):
    """
    Add a new variable for verifier public keys.
    (Initially, these are empty strings and will be appended later.)
    """
    max_verifiers = 10
    time_len = ds.sizes.get("time", 1)
    nlat = ds.sizes.get("lat", len(ds.lat))
    nlon = ds.sizes.get("lon", len(ds.lon))
    zlev_size = ds.sizes.get("zlev", 1)
    shape = (time_len, zlev_size, nlat, nlon, max_verifiers)
    logger.info(f"Adding verifier_pubkeys variable with shape {shape}")
    verifier_array = np.full(shape, "", dtype=object)
    ds["verifier_pubkeys"] = (("time", "zlev", "lat", "lon", "verifier"), verifier_array)
    return ds

def write_to_zarr(ds, zarr_store, new_time):
    """
    Write the dataset to a Zarr store on S3.
    For local testing, if the environment variable OVERWRITE_ZARR_STORE is set to true,
    the existing store is removed and a new one is created.
    Otherwise, if a store exists, the new time slice is either appended or overwrites an existing one.
    """
    logger.info(f"Preparing to write dataset to Zarr store at {zarr_store}")
    fs = fsspec.filesystem("s3", asynchronous=False)
    # Remove extra "s3://" if present.
    zarr_store_path = zarr_store.replace("s3://", "")
    store = zarr.storage.FsspecStore(fs=fs, read_only=False, path=zarr_store_path)
    
    # For local development, optionally force a new store.
    overwrite_store = os.environ.get("OVERWRITE_ZARR_STORE", "false").lower() in ("true", "1")
    if overwrite_store:
        logger.info("OVERWRITE_ZARR_STORE is true; removing existing store if any.")
        try:
            fs.rm(zarr_store_path, recursive=True)
        except Exception as e:
            logger.warning(f"Failed to remove existing store: {e}")
        logger.info("Creating a new Zarr store.")
        ds.to_zarr(store, mode="w")
        zarr.consolidate_metadata(store)
        logger.info(f"Created new Zarr store at {zarr_store}")
    else:
        try:
            existing_ds = xr.open_zarr(store, consolidated=True)
            logger.info("Existing Zarr store found.")
            existing_times = pd.to_datetime(existing_ds["time"].values)
            if new_time in existing_times:
                logger.info(f"Time slice {new_time} already exists. Overwriting it.")
                updated_ds = existing_ds.drop_sel(time=new_time)
                final_ds = xr.concat([updated_ds, ds], dim="time").sortby("time")
                final_ds.to_zarr(store, mode="w")
                zarr.consolidate_metadata(store)
                logger.info(f"Overwrote time slice {new_time} in Zarr store.")
            else:
                ds.to_zarr(store, mode="a", append_dim="time")
                logger.info(f"Appended new date {new_time} to existing Zarr store.")
        except (FileNotFoundError, zarr.errors.ContainsArrayAndGroupError) as e:
            logger.info("No existing Zarr store found or error encountered; creating a new one.")
            ds.to_zarr(store, mode="w")
            zarr.consolidate_metadata(store)
            logger.info(f"Created new Zarr store at {zarr_store}.")
    return

def convert_netcdf_to_zarr(netcdf_file, zarr_store, suffix, conversion_config=None):
    """
    Main function to convert a NetCDF file to a Zarr store.
    It loads and prepares the dataset (using conversion_config for rechunking),
    adds spatial hashes and verifier public keys, and writes the dataset to the specified Zarr store.
    """
    logger.info(f"Starting conversion for file: {netcdf_file}")
    try:
        ds, new_time = load_dataset(netcdf_file, suffix, conversion_config)
        ds = add_spatial_hashes(ds)
        ds = add_verifier_pubkeys(ds)
        write_to_zarr(ds, zarr_store, new_time)
        logger.info(f"Successfully processed and written to {zarr_store}")
    except Exception as e:
        logger.error(f"Failed to process {netcdf_file}: {str(e)}")
        raise
