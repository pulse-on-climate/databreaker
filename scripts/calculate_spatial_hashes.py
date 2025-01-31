import xarray as xr
import numpy as np
import blake3
from pathlib import Path
import struct
from typing import List, Tuple
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_logging(file_path: str = None):
    """Configure logging to both file and console"""
    if file_path:
        file_handler = logging.FileHandler(file_path)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

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
    except struct.error as e:
        logger.error(f"Error packing values: lat={lat}, lon={lon}, sst={sst_val}, "
                    f"err={err_val}, ice={ice_val}, anom={anom_val}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while hashing: {str(e)}")
        raise

def process_netcdf_file(file_path: str) -> xr.DataArray:
    """Process a single NetCDF file and calculate hashes for each lat/lon point"""
    logger.info(f"Processing file: {file_path}")
    
    try:
        # Open dataset
        ds = xr.open_dataset(file_path)
        
        # Debug print array shapes
        logger.info("\nArray shapes before squeeze:")
        logger.info(f"sst shape: {ds.sst.shape}")
        
        # Select first time step and squeeze to remove single-dimension axes
        ds = ds.isel(time=0, zlev=0)
        
        logger.info("\nArray shapes after selection:")
        logger.info(f"sst shape: {ds.sst.shape}")
        logger.info(f"Grid dimensions: {len(ds.lat)}x{len(ds.lon)}")
        
        # Initialize output array
        hash_array = np.empty((len(ds.lat), len(ds.lon)), dtype='U64')
        
        # Get variables as 2D arrays
        sst = ds.sst.values
        err = ds.err.values
        ice = ds.ice.values
        anom = ds.anom.values
        
        # Process each point
        total_points = len(ds.lat) * len(ds.lon)
        processed = 0
        
        for i in range(len(ds.lat)):
            for j in range(len(ds.lon)):
                try:
                    # Convert values to native Python floats
                    lat_val = float(ds.lat.values[i])
                    lon_val = float(ds.lon.values[j])
                    sst_val = float(sst[i,j])
                    err_val = float(err[i,j])
                    ice_val = float(ice[i,j])
                    anom_val = float(anom[i,j])
                    
                    # Calculate hash
                    hash_value = calculate_spatial_hash(
                        lat_val, lon_val, sst_val, err_val, ice_val, anom_val
                    )
                    hash_array[i,j] = hash_value
                    
                    processed += 1
                    if processed % 100000 == 0:
                        logger.info(f"Processed {processed}/{total_points} points "
                                  f"({processed/total_points*100:.1f}%)")
                    
                except Exception as e:
                    logger.error(f"Error at point ({i},{j}): {str(e)}")
                    logger.error(f"Values: lat={lat_val}, lon={lon_val}, "
                               f"sst={sst[i,j]}, err={err[i,j]}, "
                               f"ice={ice[i,j]}, anom={anom[i,j]}")
                    raise

        # Create DataArray with same coordinates as input
        hash_da = xr.DataArray(
            data=hash_array,
            dims=['lat', 'lon'],
            coords={
                'lat': ds.lat,
                'lon': ds.lon
            },
            name='spatial_hash'
        )
        
        logger.info(f"Successfully processed {file_path}")
        return hash_da
    
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {str(e)}")
        raise
    finally:
        ds.close()

def main():
    """Process test files and calculate spatial hashes"""
    setup_logging('spatial_hash_processing.log')
    
    test_files = [
        "oisst-avhrr-v02r01.20250101.nc",
        "oisst-avhrr-v02r01.20250102.nc",
        "oisst-avhrr-v02r01.20250103.nc"
    ]
    
    sample_points = [
        (0, 0, "Equator/Greenwich"),
        (25, -80, "Miami"),
        (35, 140, "Tokyo")
    ]
    
    for test_file in test_files:
        try:
            file_path = Path('tests/data') / test_file
            if not file_path.exists():
                logger.error(f"File not found: {file_path}")
                continue
            
            logger.info(f"\nProcessing {test_file}...")
            hash_da = process_netcdf_file(str(file_path))
            
            # Print sample hashes
            logger.info("\nSample hashes:")
            logger.info(f"{'Location':>20} | {'Hash':<64}")
            logger.info("-" * 90)
            
            for lat_idx, lon_idx, name in sample_points:
                try:
                    lat = float(hash_da.lat[lat_idx])
                    lon = float(hash_da.lon[lon_idx])
                    hash_value = hash_da.isel(lat=lat_idx, lon=lon_idx).item()
                    logger.info(f"{name:>20} | {hash_value}")
                except Exception as e:
                    logger.error(f"Error getting sample point {name}: {str(e)}")
            
        except Exception as e:
            logger.error(f"Error processing {test_file}: {str(e)}")

if __name__ == "__main__":
    main() 