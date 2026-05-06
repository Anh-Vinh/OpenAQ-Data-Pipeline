import argparse

from pyspark.sql import functions as F
from src.transformations.runners.spark_runner import create_spark_session
        
def process_countries(spark_session, base_path, execution_date):
    raw_locations_df = spark_session.read.option("basePath", base_path) \
                                    .json(base_path + f"location/date={execution_date}/")
    
    country_df = raw_locations_df.select(
        F.col("country").getItem("id").alias("country_id"),
        F.col("country").getItem("name").alias("country_name"),
        F.col("country").getItem("code").alias("country_code"),
        F.current_timestamp().alias("ingestion_timestamp_utc")
    ).withColumn("execution_date", F.to_date(F.lit(execution_date), "yyyy-MM-dd"))
    
    country_df = country_df.dropDuplicates(["country_id"])
    country_df = country_df.dropna(subset=["country_id"])

    country_df.write \
        .mode("append") \
        .parquet(f"s3a://silver/openaq/country/")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument("--base_path", required=True)
    parser.add_argument("--execution_date", required=True)
    
    args = parser.parse_args()
        
    base_path = args.base_path
    execution_date = args.execution_date
    
    spark_session = create_spark_session()
    print("Get/Created Spark Session")
    
    process_countries(spark_session, base_path, execution_date)
    print(f"Country data written to silver layer (MinIO)")