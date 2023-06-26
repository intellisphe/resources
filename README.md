# Sphere Data Fetching and Processing Script

This script fetches blocklog data from the Sphere API, processes it, and optionally syncs it to an S3 bucket. It requires specific start and end timestamps (in UTC epoch format) for data retrieval.

## Installation

Before running the script, ensure you have the necessary Python libraries installed. You can install them using pip:

```
pip install pandas boto3 requests argparse
```

## Configuration

The script requires a set of configurations. These include:

1. **Sphere Configuration**: This is the API key for the Sphere API, defined in the script as `API_KEY`. Replace the API key with your own.

2. **S3 Configuration**: If you intend to sync the data to an S3 bucket, specify the bucket name, AWS access key ID, AWS secret access key, and AWS region name. They are defined in the script as `S3_BUCKET_NAME`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION_NAME` respectively. Replace them with your own.

## Running the script

The script accepts the start and end timestamps as command-line arguments. Here's an example of how to run the script:

```
python sphereapi.py --start 1685617920 --end 1685617980
```


## Script Overview

Here's a brief overview of what the script does:

1. Fetches the schema and the average ad size from the Sphere API.
2. Fetches blocklog data from the Sphere API for the provided start and end timestamps.
3. Processes the data into a pandas DataFrame using the fetched schema.
4. Saves the DataFrame to a CSV file named 'output.csv'.
5. Prints the DataFrame and the average ad size in bytes.
6. If an S3 bucket name is provided, syncs the CSV file to the specified S3 bucket.

Please ensure that the AWS credentials have the necessary permissions to write to the specified S3 bucket.