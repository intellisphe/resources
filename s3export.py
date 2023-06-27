import os
import boto3

class S3Export:
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_region_name):
        self.aws_access_key_id=aws_access_key_id
        self.aws_secret_access_key=aws_secret_access_key
        self.aws_region_name=aws_region_name

    def export(self, file_path, bucket):
        if not os.path.isfile(file_path):
            raise Exception(f"File {file_path} doesn't exist.")

        if not bucket:
            raise Exception("Empty bucket name.")
        
        print(f"Syncing to s3 bucket: {bucket}")

        s3 = boto3.resource(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region_name
        )

        base_name = os.path.basename(file_path)
        with open(file_path, 'rb') as data:
            s3.Bucket(bucket).put_object(Key=base_name, Body=data)
