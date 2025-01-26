#!/bin/bash

# Create clean directory
rm -rf .aws-sam/deps/python/python
mkdir -p .aws-sam/deps/python/python

# Install core dependencies first
python -m pip install \
    --platform manylinux2014_x86_64 \
    --target=.aws-sam/deps/python/python \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    "numpy>=1.26.0,<2.0.0" \
    "pandas==2.2.0" \
    "scipy==1.12.0"

# Install remaining dependencies
python -m pip install \
    --platform manylinux2014_x86_64 \
    --target=.aws-sam/deps/python/python \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    -r lambda/requirements.txt

# Create and install worker package
echo "Installing worker package..."
mkdir -p dask_workers.egg-info
cat > dask_workers.egg-info/PKG-INFO << EOL
Metadata-Version: 2.1
Name: dask-workers
Version: 0.1.0
EOL

mkdir -p .aws-sam/deps/python/python/dask_workers
cp dask_workers/worker.py .aws-sam/deps/python/python/dask_workers/
touch .aws-sam/deps/python/python/dask_workers/__init__.py

# Clean up unnecessary files
find .aws-sam/deps/python/python -type d -name "__pycache__" -exec rm -rf {} + 