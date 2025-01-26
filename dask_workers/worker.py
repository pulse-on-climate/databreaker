import numpy as np
from typing import Dict, Any
from dask.distributed import Client
import s3fs

def simple_test() -> Dict[str, str]:
    """Simple test function that doesn't require extra dependencies"""
    return {
        'status': 'success',
        'message': 'Dask cluster is operational'
    }

def test_computation(size: int = 1000) -> Dict[str, Any]:
    """Test function that performs some actual computation
    
    Args:
        size: Size of the array to create and compute on
        
    Returns:
        Dict containing computation results and stats
    """
    try:
        # Create a large array and do some computations
        data = np.random.random(size)
        result = {
            'mean': float(np.mean(data)),
            'std': float(np.std(data)),
            'min': float(np.min(data)),
            'max': float(np.max(data)),
            'size': size,
            'status': 'success'
        }
        return result
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e)
        }

def convert_netcdf_to_zarr(
    source_path: str,
    dest_path: str,
    chunk_config: Dict[str, int],
    compression_config: Dict[str, Any],
    s3_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Convert NetCDF file to Zarr format with specified configuration
    This function will be called by the Lambda via Dask scheduler
    
    Args:
        source_path: S3 path to source NetCDF file
        dest_path: S3 path for destination Zarr store
        chunk_config: Dictionary of dimension names to chunk sizes
        compression_config: Zarr compression settings
        s3_config: S3 filesystem configuration
    
    Returns:
        Dict containing conversion status and metadata
    """
    # Import dependencies only when needed
    import xarray as xr
    import zarr
    
    # Initialize S3 filesystem with localstack config
    s3 = s3fs.S3FileSystem(
        client_kwargs={
            'endpoint_url': 'http://localstack:4566'
        },
        key='test',
        secret='test'
    )
    
    try:
        # Open and chunk the dataset
        with xr.open_dataset(
            s3.open(source_path.replace('s3://', '')), 
            engine='netcdf4'
        ) as ds:
            # Apply chunking strategy
            ds = ds.chunk(chunk_config)
            
            # Create Zarr stores
            zarr_store = s3fs.S3Map(root=dest_path, s3=s3, check=False)
            zarr_temp = s3fs.S3Map(root=f"{dest_path}-temp", s3=s3, check=False)
            
            # Write to Zarr with compression
            ds.to_zarr(
                zarr_store,
                mode='w',
                consolidated=True,
                compute=True,
                encoding={
                    var: compression_config
                    for var in ds.data_vars
                }
            )
            
            # Consolidate metadata
            zarr.consolidate_metadata(zarr_store)
            
            return {
                "status": "success",
                "dimensions": dict(ds.dims),
                "variables": list(ds.data_vars),
                "chunks": ds.chunks
            }
            
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    # Remove the polling loop - workers shouldn't poll SQS
    ...