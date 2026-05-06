import argparse

from pyspark.sql import functions as F
from src.transformations.runners.spark_runner import create_spark_session

def process_measurements(spark_session, base_path, execution_date):
    raw_measurments_df = spark_session.read.option("basePath", base_path) \
                                        .json(base_path + f"measurement/date={execution_date}/")
    
    measurement_df = raw_measurments_df.select(
        F.col("sensor_id"),
        F.col("value"),
        # F.col("date"),
        F.col("parameter").getItem("id").alias("param_id"),
        # F.col("parameter").getItem("name").alias("param_name"),
        # F.col("parameter").getItem("units").alias("param_units"),
        # F.col("parameter").getItem("displayName").alias("param_display_name"),
        F.col("summary").getItem("avg").alias("avg"),
        F.col("summary").getItem("max").alias("max"),
        F.col("summary").getItem("min").alias("min"),
        F.col("coverage.datetimeFrom").getItem("utc").alias("datetime_from_utc"),
        F.col("coverage.datetimeTo").getItem("utc").alias("datetime_to_utc"),
        F.current_timestamp().alias("ingestion_timestamp_utc")
    ).withColumn("datetime_from_utc", F.to_timestamp("datetime_from_utc", "yyyy-MM-dd'T'HH:mm:ssX")) \
    .withColumn("datetime_to_utc", F.to_timestamp("datetime_to_utc", "yyyy-MM-dd'T'HH:mm:ssX")) \
    .withColumn("measure_date", F.to_date(F.col("datetime_from_utc"))) \
    .withColumn("file_path", F.input_file_name()) \
    .withColumn("execution_date", F.to_date(F.lit(execution_date), "yyyy-MM-dd"))
    
    # measurement_df = measurement_df.withColumn("sensor_id", F.regexp_extract(F.col("file_path"), r"([^/]+)\.json$", 1).cast("long"))

    measurement_df.write \
        .mode("append") \
        .partitionBy("measure_date") \
        .parquet(f"s3a://silver/openaq/measurement/")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--base_path", required=True)
    parser.add_argument("--execution_date", required=True)
    
    args = parser.parse_args()
        
    base_path = args.base_path
    execution_date = args.execution_date
    
    spark_session = create_spark_session()
    print("Get/Created Spark Session")
    
    process_measurements(spark_session, base_path, execution_date)
    print(f"Measurement data written to silver layer (MinIO)")