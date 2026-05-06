import argparse

from pyspark.sql import functions as F
from src.transformations.runners.spark_runner import create_spark_session
        
def process_sensors(spark_session, base_path, execution_date):
    raw_locations_df = spark_session.read.option("basePath", base_path) \
                                    .json(base_path + f"location/date={execution_date}/")
                                    
    sensor_df = raw_locations_df.select(
        F.col("id").alias("location_id"),
        F.explode("sensors").alias("sensors")
    )
    
    sensor_df = sensor_df.select(
        F.col("sensors").getItem("id").alias("sensor_id"),
        F.col("location_id"),
        F.col("sensors").getItem("name").alias("sensor_name"),
        F.col("sensors").getItem("parameter").getItem("id").alias("param_id"),
        # F.col("sensors").getItem("parameter").getItem("name").alias("param_name"),
        # F.col("sensors").getItem("parameter").getItem("units").alias("param_units"),
        # F.col("sensors").getItem("parameter").getItem("displayName").alias("param_display_name"),
        F.current_timestamp().alias("ingestion_timestamp_utc")
    ).withColumn("execution_date", F.to_date(F.lit(execution_date), "yyyy-MM-dd"))
    
    sensor_df = sensor_df.dropDuplicates(["sensor_id"])
    sensor_df = sensor_df.dropna(subset=["sensor_id"])
    
    sensor_df.write \
        .mode("append") \
        .parquet(f"s3a://silver/openaq/sensor/")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--base_path", required=True)
    parser.add_argument("--execution_date", required=True)
    
    args = parser.parse_args()

    base_path = args.base_path
    execution_date = args.execution_date
    
    spark_session = create_spark_session()
    print("Get/Created Spark Session")
    
    process_sensors(spark_session, base_path, execution_date)
    print(f"Sensor data written to silver layer (MinIO)")