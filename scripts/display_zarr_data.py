import xarray as xr
import pandas as pd
import numpy as np
from datetime import datetime
import random
import sys
from fsspec.core import get_fs_token_paths
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

def plot_sst_grid(sst_data, date):
    """
    Plot SST data as a heatmap with NaN values in grey.
    
    Args:
        sst_data: xarray DataArray containing SST values
        date: string representing the date of the data
    """
    # Create a custom colormap with more varied colors
    colors = ['navy', 'blue', 'cyan', 'lightcyan', 
              'yellow', 'orange', 'red', 'darkred']
    n_bins = 256  # Increased for smoother color transitions
    cmap = mcolors.LinearSegmentedColormap.from_list("custom", colors, N=n_bins)
    
    # Set background color for NaN values
    cmap.set_bad('lightgrey')
    
    # Create a masked array where NaN values will be grey and squeeze out time dimension
    sst_array = sst_data.values.squeeze()
    
    # Rearrange the array so longitude 0° is in the middle
    # The array is 1440 points wide, so index 720 corresponds to 0°
    left_half = sst_array[:, 720:]  # from 0° to 180°E
    right_half = sst_array[:, :720]  # from 180°W to 0°
    sst_array = np.concatenate([left_half, right_half], axis=1)
    
    masked_data = np.ma.masked_where(np.isnan(sst_array), sst_array)
    
    # Create the plot
    plt.figure(figsize=(15, 8))
    plt.imshow(masked_data, 
               cmap=cmap,
               aspect='auto',
               interpolation='none',
               origin='lower')
    
    # Add a colorbar
    cbar = plt.colorbar()
    cbar.set_label('Sea Surface Temperature (°C)')
    
    # Set title and labels
    plt.title(f'Sea Surface Temperature Grid - {date}')
    plt.xlabel('Longitude (180°W to 180°E)')
    plt.ylabel('Latitude (90°S to 90°N)')
    
    # Customize x-axis ticks to show longitude values
    x_ticks = np.linspace(0, 1440, 9)
    x_labels = [f'{int(lon)}°' for lon in np.linspace(-180, 180, 9)]
    plt.xticks(x_ticks, x_labels)
    
    # Add vertical line at longitude 0°
    plt.axvline(x=720, color='black', linestyle='--', alpha=0.3)
    
    # Show the plot
    plt.show()
    plt.close()

def display_zarr_data(zarr_path: str, storage_options: dict = None):
    """Display SST data from Zarr store with formatted output"""
    if storage_options is None:
        storage_options = {
            'anon': False 
        }
    
    print(f"\nReading Zarr data from {zarr_path}...")
    
    try:
        # Ensure the path is properly formatted for s3fs
        if zarr_path.startswith('s3://'):
            zarr_path = zarr_path.replace('s3://', '')
        
        # Check if the store exists before trying to open it
        fs, _, _ = get_fs_token_paths(f's3://{zarr_path}', storage_options=storage_options)
        if not fs.exists(zarr_path):
            print(f"Error: Zarr store not found at path: {zarr_path}")
            print("Please check if the path is correct and you have proper access permissions.")
            sys.exit(1)
            
        ds = xr.open_zarr(f's3://{zarr_path}', storage_options=storage_options)
        
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
            spatial_hashes = ds.spatial_hash.sel(time=time)
            
            print(f"\n{'='*80}")
            print(f"Date: {date}")
            print(f"{'='*80}")
            
            # Add this line to plot the SST grid
            plot_sst_grid(daily_sst, date)
            
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
            
            # Display sample coordinates with SST and hash values
            print("\nSample Coordinates with SST and Spatial Hash Values:")
            print(f"{'Latitude':>10} {'Longitude':>10} {'SST (°C)':>10} {'Spatial Hash':>12}")
            print("-" * 45)
            
            # Get computed arrays for faster access
            sst_data = daily_sst.compute()
            hash_data = spatial_hashes.compute()
            
            # Print the shapes to understand the structure
            print(f"\nArray shapes:")
            print(f"SST data shape: {sst_data.shape}")
            print(f"Latitude shape: {ds.lat.shape}")
            print(f"Longitude shape: {ds.lon.shape}")
            
            num_samples = 5
            for _ in range(num_samples):
                # Get random indices within the correct dimensions
                lat_idx = random.randrange(ds.lat.size)
                lon_idx = random.randrange(ds.lon.size)
                
                # Access the data using the correct dimension order
                lat = float(ds.lat[lat_idx].values.item())
                lon = float(ds.lon[lon_idx].values.item())
                
                # Use item() to properly extract single values
                sst = sst_data.sel(lat=lat, lon=lon).values.item()
                hash_val = str(hash_data.sel(lat=lat, lon=lon).values.item())
                
                print(f"{lat:10.2f} {lon:10.2f} {sst:10.2f} {hash_val:>12}")
            
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
        
    except FileNotFoundError as e:
        print(f"Error: Could not find or access the Zarr store at {zarr_path}")
        print("Please check if:")
        print("  1. The path is correct")
        print("  2. You have proper AWS credentials configured")
        print("  3. The S3 bucket exists and is accessible")
        print(f"\nDetailed error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)
    finally:
        # Clean up any remaining sessions
        try:
            fs.close()
        except:
            pass

if __name__ == "__main__":
    zarr_path = "s3://databreaker-source-zarr"
    try:
        display_zarr_data(zarr_path)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(0) 