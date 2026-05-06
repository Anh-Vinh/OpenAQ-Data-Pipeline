import uuid
import json

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def prepare_bucket(bucket_name):
    try:
        s3 = S3Hook("minio_conn")
        
        if not s3.check_for_bucket(bucket_name):
            s3.create_bucket(bucket_name=bucket_name)
            print(f"Created bucket: {bucket_name}")
        else:
            print(f"Bucket already exists: {bucket_name}")
        
    except Exception as e:
        print(f"Error while checking MinIO bucket: {e}")

def write_to_minio(data, entity_type, execution_date, bucket_name="bronze"):
    try:
        s3 = S3Hook("minio_conn")
        
        object_key = f"openaq/{entity_type}/date={execution_date}/part-{uuid.uuid4()}.json"
        
        json_data = json.dumps(data)
        
        s3.load_string(
            string_data=json_data,
            key=object_key,
            bucket_name=bucket_name,
            replace=True
        )
        
        print(f"Uploaded {entity_type} to MinIO")
        
    except Exception as e:
        print(f"Error while upload {entity_type} to MinIO: {e}")