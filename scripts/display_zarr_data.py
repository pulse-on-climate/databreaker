import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime

def display_zarr_data(zarr_path: str, storage_options: dict = None):
    """Display SST data from Zarr store with formatted output"""
    if storage_options is None:
        storage_options = {
            'client_kwargs': {'endpoint_url': 'http://localhost:4566'},
            'anon': True
        }
    else:
        storage_options = {
            'client_kwargs': {'endpoint_url': storage_options['endpoint_url']},
            'anon': True
        }
    
    print(f"\nReading Zarr data from {zarr_path}...")
    ds = xr.open_zarr(zarr_path, storage_options=storage_options)
    
    # Print dataset overview
    print("\nDataset Overview:")
    print(f"Time range: {ds.time.values[0]} to {ds.time.values[-1]}")
    print(f"Number of timesteps: {len(ds.time)}")
    print(f"Grid dimensions: {len(ds.lat)}x{len(ds.lon)} (lat x lon)")
    
    # Calculate and store daily averages for running mean
    daily_means = []
    
    # Process each day
    for i, time in enumerate(ds.time.values):
        date = pd.Timestamp(time).strftime('%Y-%m-%d')
        daily_sst = ds.sst.sel(time=time)
        
        print(f"\n{'='*80}")
        print(f"Date: {date}")
        print(f"{'='*80}")
        
        # Calculate statistics
        stats = daily_sst.compute()  # Compute once for all statistics
        daily_mean = float(stats.mean())
        daily_min = float(stats.min())
        daily_max = float(stats.max())
        daily_means.append(daily_mean)
        
        print(f"\nDaily Statistics:")
        print(f"Min SST: {daily_min:.2f}°C")
        print(f"Max SST: {daily_max:.2f}°C")
        print(f"Average SST: {daily_mean:.2f}°C")
        
        # Calculate running average
        running_mean = np.mean(daily_means)
        print(f"\nRunning Average SST (Day 1 to {i+1}): {running_mean:.2f}°C")
        
        # Print running average components
        print(f"Days included in running average: ", end="")
        for j, dm in enumerate(daily_means):
            print(f"Day {j+1}: {dm:.2f}°C", end="")
            if j < len(daily_means) - 1:
                print(" + ", end="")
        print(f" = {running_mean:.2f}°C")

if __name__ == "__main__":
    zarr_path = "s3://noaa-oisst-zarr"
    display_zarr_data(zarr_path) 