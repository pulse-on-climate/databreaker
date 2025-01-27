import xarray as xr
import s3fs
import os
import tempfile
import zarr

def convert_netcdf_to_zarr(source_path: str, dest_path: str):
    """
    Convert a single NetCDF file to Zarr format
    
    Args:
        source_path: S3 path to source NetCDF file (s3://bucket/key)
        dest_path: S3 path for output Zarr store (s3://bucket/key)
    """
    # Initialize S3 filesystem
    s3 = s3fs.S3FileSystem(
        endpoint_url=os.environ.get('AWS_ENDPOINT_URL', 'http://localstack:4566'),
        key=os.environ.get('AWS_ACCESS_KEY_ID', 'test'),
        secret=os.environ.get('AWS_SECRET_ACCESS_KEY', 'test')
    )
    
    # Create a temporary directory for zarr store
    with tempfile.TemporaryDirectory() as tmpdir:
        # First download the file locally
        print(f"Downloading {source_path}...")
        nc_file = os.path.join(tmpdir, 'input.nc')
        s3.get(source_path.replace('s3://', ''), nc_file)
        
        # Create local zarr store
        zarr_path = os.path.join(tmpdir, 'output.zarr')
        
        # Open and convert
        print(f"Converting to Zarr...")
        with xr.open_dataset(nc_file, engine='netcdf4') as ds:
            print(f"Dataset loaded: {ds}")
            print(f"Dataset variables: {list(ds.variables)}")
            print(f"Dataset dimensions: {ds.dims}")
            
            # Convert to Zarr
            ds.to_zarr(
                store=zarr_path,
                mode='w'
            )
            print("Dataset written to Zarr store")
            
            # Upload to S3
            print(f"Uploading to {dest_path}...")
            s3.put(zarr_path, dest_path.replace('s3://', ''), recursive=True)
    
    print(f"Conversion complete: {source_path} -> {dest_path}")
    return {
        "source": source_path,
        "destination": dest_path
    } 