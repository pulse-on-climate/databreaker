import os
import sys
from distributed import Client
from dask_workers.worker import convert_netcdf_to_zarr

def main():
    """Main entry point for the client container"""
    try:
        # Get environment variables
        bucket = os.environ['INPUT_BUCKET']
        key = os.environ['INPUT_KEY']
        scheduler = os.environ['DASK_SCHEDULER']
        
        # Connect to Dask scheduler
        print(f"Connecting to Dask scheduler at {scheduler}...")
        client = Client(scheduler)
        print("Connected to Dask cluster")
        
        # Submit conversion task
        print(f"Processing file s3://{bucket}/{key}")
        future = client.submit(
            convert_netcdf_to_zarr,
            source_path=f's3://{bucket}/{key}',
            dest_path=f's3://noaa-oisst-zarr/{key.replace(".nc", "")}',
            chunk_config={'time': 1},
            compression_config={'compressor': None},
            s3_config={}
        )
        
        # Wait for result
        result = future.result()
        print(f"Conversion completed: {result}")
        sys.exit(0)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main() 