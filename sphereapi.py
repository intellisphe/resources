import os
import boto3
import requests
import pandas as pd

# Sphere configuration
BASE_URL = "https://api.getsphere.ai/blocklogs"
API_KEY = 'your_api_key_here'  # replace with your API key

# S3 Configuration
S3_BUCKET_NAME = 'your_s3_bucket_name' # your S3 bucket name
AWS_ACCESS_KEY_ID = 'your_aws_access_key' # your access key id
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_access_ket' # your secret access key
AWS_REGION_NAME = 'us-east-1' # your region name

def fetch_data(start, end):
    url = f"{BASE_URL}/data?start={start}&end={end}"
    headers = {'x-api-key': API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.json())
        return None

def fetch_schema():
    url = f"{BASE_URL}/schema"
    headers = {'x-api-key': API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.json())
        return None

def fetch_adsize(type):
    url = f"{BASE_URL}/adsize?type={type}"
    headers = {'x-api-key': API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(response.json())
        return None

def create_dataframe(data, schema):
    column_names = [col['name'] for col in schema['table']['columns']]
    df = pd.DataFrame(data['data'], columns=column_names)
    return df

def sync_s3(file_path, bucket):
    if not os.path.isfile(file_path):
        raise Exception(f"File {file_path} doesn't exist.")

    if not bucket:
        raise Exception("Empty bucket name.")
    
    print(f"Syncing to s3 bucket: {bucket}")

    s3 = boto3.resource(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME
    )

    base_name = os.path.basename(file_path)
    with open(file_path, 'rb') as data:
        s3.Bucket(bucket).put_object(Key=base_name, Body=data)

def main(start, end):
    schema = fetch_schema()
    if schema is None:
        print('Failed to fetch schema.')
        return

    adsize = fetch_adsize('video')
    if adsize is None:
        print('Failed to fetch ad size.')
        return

    data = fetch_data(start, end)
    if data is None:
        print('Failed to fetch data.')
        return

    df = create_dataframe(data, schema)

    print(df)
    df.to_csv('output.csv', index=False)

    vdf = df[df['ad_position']=='VIDEO']
    length = len(vdf['ad_position'])
    vsize = length * adsize["average_size_bytes"]
    print(f"\n\nAverage ad size in bytes {vsize}\n\n")

    if S3_BUCKET_NAME:
        file_path = os.path.join(os.getcwd(), 'output.csv')
        sync_s3(file_path, S3_BUCKET_NAME)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch data from Sphere API.')
    parser.add_argument('-s', '--start', type=int, required=True, help='Start UTC epoch')
    parser.add_argument('-e', '--end', type=int, required=True, help='End UTC epoch')

    args = parser.parse_args()

    main(args.start, args.end)