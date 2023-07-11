import os
import pandas as pd
from argparse import ArgumentParser

from sphereapi import SphereApi
from s3export import S3Export
from influxexport import InfluxExport

# Sphere configuration
API_KEY = 'your_api_key_here'  # replace with your API key

# S3 Configuration
AWS_ACCESS_KEY_ID = 'your_aws_access_key' # your access key id
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_access_ket' # your secret access key
AWS_REGION_NAME = 'us-east-1' # your region name
S3_BUCKET = None # your S3 bucket name

# InfluxDB Configuration
INFLUXDB_URL = "http://localhost:8086"  # replace with your InfluxDB URL
INFLUXDB_TOKEN = "your_token"  # replace with your InfluxDB token
INFLUXDB_ORG = "your_org"  # replace with your InfluxDB org
INFLUXDB_BUCKET = None  # replace with your InfluxDB bucket

def create_dataframe(data, schema):
    column_names = [col['name'] for col in schema['table']['columns']]
    df = pd.DataFrame(data['data'], columns=column_names)
    return df

def main(start, end):
    api = SphereApi(
        api_key=API_KEY)

    schema = api.schema()
    adsize = api.adsize('video')
    data = api.data(start, end)
    df = create_dataframe(data, schema)

    print(df)
    df.to_csv('output.csv', index=False)

    vdf = df[df['ad_position']=='VIDEO']
    length = len(vdf['ad_position'])
    vsize = length * adsize["average_size_bytes"]
    print(f"Average ad size in bytes {vsize}")

    if S3_BUCKET:
        s3 = S3Export(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_region_name=AWS_REGION_NAME)
        file_path = os.path.join(os.getcwd(), 'output.csv')
        s3.export(file_path, S3_BUCKET)

    if INFLUXDB_BUCKET:
        influx = InfluxExport(
            influxdb_url=INFLUXDB_URL,
            influxdb_token=INFLUXDB_TOKEN,
            influxdb_org=INFLUXDB_ORG,
            influxdb_bucket=INFLUXDB_BUCKET)
        influx.export(data=df, schema=schema)


if __name__ == '__main__':
    parser = ArgumentParser(description='Fetch data from Sphere API.')
    parser.add_argument('-s', '--start', type=int, required=True, help='Start UTC epoch')
    parser.add_argument('-e', '--end', type=int, required=True, help='End UTC epoch')

    args = parser.parse_args()

    main(args.start, args.end)