from distributed import Client
import time
import dask_workers.worker
import sys
import importlib

def monitor_task(future, timeout=60):
    """Monitor a task with proper error handling and timeout"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            status = future.status
            if status == "finished":
                return future.result()
            elif status == "error":
                raise future.exception()
            elif status == "cancelled":
                raise Exception("Task was cancelled")
            print(f"Task status: {status}")
            time.sleep(2)
        except Exception as e:
            print(f"❌ Task failed: {e}")
            raise
    raise TimeoutError(f"Task did not complete within {timeout} seconds")

def test_dask_connection():
    # Connect to Dask scheduler
    client = Client("tcp://dask-scheduler:8786")
    
    # Submit a simple test computation
    future = client.submit(lambda x: x + 1, 10)
    result = future.result()
    
    return {
        'message': 'Dask cluster is operational',
        'test_result': result
    }

def main():
    try:
        # Connect to scheduler
        client = Client("tcp://dask-scheduler:8786")
        print(f"Connected to {client}")
        print(f"Client environment:")
        print(f"Python: {sys.version}")
        print(f"Packages:")
        for pkg in ['dask', 'distributed', 'numpy', 'pandas']:
            try:
                mod = importlib.import_module(pkg)
                print(f"  {pkg}: {mod.__version__}")
            except:
                print(f"  {pkg}: not found")

        print("\nScheduler environment:")
        print(client.get_versions())

        # Submit test computation
        print("Submitting test computation...")
        future = client.submit(dask_workers.worker.simple_test)
        
        # Wait for workers and monitor task
        print("Waiting for task completion...")
        result = monitor_task(future)
        
        # Verify result structure
        if not isinstance(result, dict) or result.get("status") != "success":
            raise ValueError(f"Invalid result format: {result}")
            
        print("✅ Test successful:")
        print(f"  Message: {result['message']}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        exit(1)

if __name__ == "__main__":
    result = test_dask_connection()
    print(result) 