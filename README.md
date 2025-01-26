# databreaker

A serverless AWS infrastructure for automatically converting NOAA CDR Sea Surface Temperature NetCDF files to Zarr format using Dask distributed computing.

## Overview

This project sets up an AWS infrastructure that:
1. Monitors an S3 bucket for new NetCDF files from NOAA's CDR Sea Surface Temperature dataset
2. Automatically triggers a conversion process when new files are detected
3. Uses Dask on ECS for distributed processing
4. Stores the converted Zarr files in a destination bucket

Source data: [NOAA CDR Sea Surface Temperature](https://noaa-cdr-sea-surface-temp-optimum-interpolation-pds.s3.amazonaws.com/index.html#data/v2.1/avhrr/202501/)

## Prerequisites Installation

1. Install Docker Desktop from [docker.com](https://www.docker.com/products/docker-desktop)

2. Install Python 3.9+ from [python.org](https://www.python.org/downloads/)

3. Install Node.js from [nodejs.org](https://nodejs.org/)

4. Install AWS CLI:
   - Download from [aws.amazon.com/cli](https://aws.amazon.com/cli/)
   - Configure with `aws configure`

5. Install project dependencies:
`python -m venv .venv`
`source .venv/bin/activate`
`pip install -r requirements.txt`
`npm install -g aws-cdk`

## Troubleshooting

### LocalStack Issues
- Ensure Docker is running
- Check LocalStack logs: `localstack logs`
- Verify LocalStack services: `localstack status services`

### Deployment Issues
- Verify AWS credentials
- Check CloudFormation console for stack status
- Review CloudWatch logs for Lambda functions

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License - See LICENSE file for details