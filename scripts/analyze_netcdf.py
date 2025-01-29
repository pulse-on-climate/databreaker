import xarray as xr
import numpy as np
from typing import Dict, Any
import json

def analyze_netcdf(file_path: str) -> Dict[str, Any]:
    """
    Analyze a NetCDF file and suggest optimal Zarr configuration
    
    Args:
        file_path: Path to NetCDF file
    
    Returns:
        Dictionary with suggested Zarr configuration
    """
    print(f"Analyzing {file_path}...")
    
    with xr.open_dataset(file_path) as ds:
        # Get basic dataset info
        print("\nDataset Overview:")
        print("================")
        print(f"Dimensions: {dict(ds.dims)}")
        print(f"Data variables: {list(ds.data_vars)}")
        print(f"Coordinates: {list(ds.coords)}")
        
        # Analyze overall dataset characteristics
        all_floating = all(np.issubdtype(var.dtype, np.floating) 
                         for var in ds.data_vars.values())
        
        # Determine single compressor for entire array
        if all_floating:
            compressor = {
                'id': 'blosc',
                'cname': 'lz4',  # Fast compression
                'clevel': 5,
                'shuffle': 2  # BITSHUFFLE for floating point
            }
        else:
            compressor = None
        
        # Analyze each variable
        config = {
            "dataset": "custom",  # Can be customized
            "description": ds.attrs.get('title', 'Custom dataset'),
            "compressor": compressor,  # Single compressor for all variables
            "conversion": {
                "variables": {}
            }
        }
        
        for var_name, var in ds.data_vars.items():
            print(f"\nAnalyzing variable: {var_name}")
            print(f"Shape: {var.shape}")
            print(f"Dtype: {var.dtype}")
            
            # Suggest chunks based on variable size and dimensions
            chunks = {}
            for dim_name, dim_size in zip(var.dims, var.shape):
                if dim_name == 'time':
                    chunks[dim_name] = 1
                else:
                    target_chunk_size = 100 * 1024 * 1024
                    bytes_per_value = var.dtype.itemsize
                    chunk_size = int(np.sqrt(target_chunk_size / bytes_per_value))
                    chunks[dim_name] = min(chunk_size, dim_size)
            
            config["conversion"]["variables"][var_name] = {
                "chunks": chunks
            }
        
        return config

def main():
    """Command line interface"""
    import argparse
    parser = argparse.ArgumentParser(description='Analyze NetCDF file and suggest Zarr configuration')
    parser.add_argument('file', help='Path to NetCDF file')
    parser.add_argument('--output', '-o', help='Output JSON file for configuration')
    args = parser.parse_args()
    
    config = analyze_netcdf(args.file)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"\nConfiguration written to {args.output}")
    else:
        print("\nSuggested Configuration:")
        print("=======================")
        print(json.dumps(config, indent=2))

if __name__ == '__main__':
    main() 