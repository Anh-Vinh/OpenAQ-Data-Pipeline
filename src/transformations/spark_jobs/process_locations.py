import argparse

from pyspark.sql import functions as F
from src.transformations.runners.spark_runner import create_spark_session
        
def process_locations(spark_session, base_path, execution_date):
    raw_locations_df = spark_session.read.option("basePath", base_path) \
                                    .json(base_path + f"location/date={execution_date}/")
                                    
    location_df = raw_locations_df.select(
        F.col("id").alias("location_id"),
        F.col("country").getItem("id").alias("country_id"),
        F.col("name").alias("location_name"),
        F.col("locality").alias("locality"),
        F.col("timezone").alias("timezone"),
        F.col("datetimeFirst").getItem("utc").alias("datetime_first_utc"),
        F.col("datetimeLast").getItem("utc").alias("datetime_last_utc"),
        F.current_timestamp().alias("ingestion_timestamp_utc")
    ).withColumn("datetime_first_utc", F.to_timestamp("datetime_first_utc", "yyyy-MM-dd'T'HH:mm:ssX")) \
    .withColumn("datetime_last_utc", F.to_timestamp("datetime_last_utc", "yyyy-MM-dd'T'HH:mm:ssX")) \
    .withColumn("execution_date", F.to_date(F.lit(execution_date), "yyyy-MM-dd"))
    
    location_df = location_df.dropDuplicates(["location_id"])
    location_df = location_df.dropna(subset=["location_id"])
    
    location_df.write \
        .mode("append") \
        .parquet(f"s3a://silver/openaq/location/")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--base_path", required=True)
    parser.add_argument("--execution_date", required=True)
    
    args = parser.parse_args()
        
    base_path = args.base_path
    execution_date = args.execution_date
    
    spark_session = create_spark_session()
    print("Get/Created Spark Session")
    
    process_locations(spark_session, base_path, execution_date)
    print(f"Location data written to silver layer (MinIO)")