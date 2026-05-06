import os
import argparse

from dotenv import load_dotenv
from pyspark.sql import functions as F
from src.transformations.runners.spark_runner import create_spark_session

load_dotenv()

db_host = os.getenv("POSTGRES_HOST")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

def insert_to_staging(spark_session, base_path, jdbc_url, db_properties, entity_type, table_name):
    if entity_type == "measurement":
        entity_df = spark_session.read.parquet(base_path + f"/measurement/")
    else:
        entity_df = spark_session.read.parquet(base_path + f"/{entity_type}/*.parquet")
    
    entity_df.write \
        .mode("append") \
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=db_properties
        )
    print(f"Inserted to {table_name}")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--base_path", required=True)
    parser.add_argument("--entity_type", required=True)
    parser.add_argument("--table_name", required=True)
    
    args = parser.parse_args()
        
    base_path = args.base_path
    entity_type = args.entity_type
    table_name = args.table_name
    
    jdbc_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"
    
    db_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
        "batchsize": "5000"
    }
    
    spark_session = create_spark_session()
    print("Get/Created Spark Session")
    
    insert_to_staging(spark_session, base_path, jdbc_url, db_properties, entity_type, table_name)