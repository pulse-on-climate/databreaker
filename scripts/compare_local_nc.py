import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def analyze_local_netcdf():
    """Analyze local NetCDF files and display statistics matching display_zarr_data output"""
    
    # Define test files
    test_files = [
        "oisst-avhrr-v02r01.20250101.nc",
        "oisst-avhrr-v02r01.20250102.nc",
        "oisst-avhrr-v02r01.20250103.nc"
    ]
    
    print("\nDataset Overview:")
    
    # Store daily means for running average
    daily_means = []
    
    # Process each file
    for test_file in test_files:
        file_path = Path('tests/data') / test_file
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            continue
            
        print(f"\n{'='*80}")
        print(f"Date: {test_file.split('.')[-2]}")
        print(f"{'='*80}")
        
        # Read the NetCDF file
        with xr.open_dataset(file_path) as ds:
            # Calculate statistics
            daily_sst = ds.sst
            daily_min = float(daily_sst.min())
            daily_max = float(daily_sst.max())
            daily_mean = float(daily_sst.mean())
            daily_means.append(daily_mean)
            
            print(f"\nDaily Statistics:")
            print(f"Min SST: {daily_min:.2f}°C")
            print(f"Max SST: {daily_max:.2f}°C")
            print(f"Average SST: {daily_mean:.2f}°C")
            
            # Calculate running average
            running_mean = np.mean(daily_means)
            print(f"\nRunning Average SST (Day 1 to {len(daily_means)}): {running_mean:.2f}°C")
            
            # Print running average components
            print(f"Days included in running average: ", end="")
            for j, dm in enumerate(daily_means):
                print(f"Day {j+1}: {dm:.2f}°C", end="")
                if j < len(daily_means) - 1:
                    print(" + ", end="")
            print(f" = {running_mean:.2f}°C")

if __name__ == "__main__":
    analyze_local_netcdf() 