import os
import subprocess

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY")

def create_spark_session(app_name="OpenAQ_Pipeline"):
    spark_config = {
        "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
        "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
        "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.sql.caseSensitive": "true"
    }

    builder = (
        SparkSession.builder
        .appName(app_name)
    )

    for key, value in spark_config.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()

def run_spark_job(job_path: str, args: list = None):
    jars = ",".join([
        "/opt/airflow/spark_jars/hadoop-aws-3.3.4.jar",
        "/opt/airflow/spark_jars/aws-java-sdk-bundle-1.12.262.jar",
        "/opt/airflow/spark_jars/postgresql-42.6.0.jar"
    ])
    
    cmd = [
        "spark-submit", 
        "--jars", jars,
        job_path
    ]
    
    if args:
        cmd.extend(args) 
    
    subprocess.run(cmd, check=True)