import os
import boto3

# S3 Configuration
AWS_ACCESS_KEY_ID = 'your_aws_access_key' # your access key id
AWS_SECRET_ACCESS_KEY = 'your_aws_secret_access_ket' # your secret access key
AWS_REGION_NAME = 'us-east-1' # your region name

S3_BUCKET = None # your S3 bucket name

class S3Export:
    def __init__(self,
        aws_access_key_id = AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY,
        aws_region_name = AWS_REGION_NAME):
        
        self.aws_access_key_id=aws_access_key_id
        self.aws_secret_access_key=aws_secret_access_key
        self.aws_region_name=aws_region_name
        self.s3client = boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name
        )

    def export(self, file_path, bucket = S3_BUCKET):
        if not bucket:
            return

        if not os.path.isfile(file_path):
            raise Exception(f"File {file_path} doesn't exist.")

        if not bucket:
            raise Exception("Empty bucket name.")
        
        print(f"Syncing to s3 bucket: {bucket}")

        base_name = os.path.basename(file_path)
        with open(file_path, 'rb') as data:
            self.s3client.Bucket(bucket).put_object(Key=base_name, Body=data)
