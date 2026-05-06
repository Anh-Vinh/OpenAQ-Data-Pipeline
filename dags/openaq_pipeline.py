from airflow.sdk import dag, task
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

@dag(
    dag_id="openaq_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False
)
def openaq_pipeline():    
    @task
    def prepare_buckets(ds=None):
        from src.storage.minio_client import prepare_bucket
        
        prepare_bucket("bronze")
        prepare_bucket("silver")
        print(f"Buckets are ready!")
        
    @task
    def prepare_tables(ds=None):
        from airflow.sdk.bases.hook import BaseHook
        from src.transformations.runners.sql_runner import run_sql_file
        
        try:
            conn = BaseHook.get_connection("postgres_conn")
            conn_params = {
                "host": conn.host,
                "dbname": conn.schema,
                "user": conn.login,
                "password": conn.password,
                "port": conn.port
            }
            
            run_sql_file(conn_params, f"src/transformations/sql/init.sql")
            print(f"Tables are ready!")
            
        except Exception as e:
            print(f"Error while preparing postgres tables: {e}")
    
    @task
    def ingest_data(ds=None):
        from src.ingestion.fetch_api import fetch_active_locations, fetch_measurements
        from src.storage.minio_client import write_to_minio
        
        try:
            locations = fetch_active_locations()
            measurements = fetch_measurements(locations, ds)

            write_to_minio(locations, "location", ds)
            write_to_minio(measurements, "measurement", ds)

        except Exception as e:
            print(f"Error while ingest data to MinIO: {e}")
            
    @task
    def process_entity(entity_type, ds=None):
        from src.transformations.runners.spark_runner import run_spark_job 
        
        job_path = f"src/transformations/spark_jobs/process_{entity_type}.py"
        base_path = f"s3a://bronze/openaq/"
        
        run_spark_job(
            job_path=job_path,
            args=[
                "--base_path", base_path,
                "--execution_date", ds
            ]
        )
        
    @task
    def insert_entity(entity_type, ds=None):
        from src.transformations.runners.spark_runner import run_spark_job
        
        job_path = "src/transformations/spark_jobs/insert_to_staging.py"
        base_path = f"s3a://silver/openaq/"
        
        run_spark_job(
            job_path=job_path,
            args=[
                "--base_path", base_path,
                "--entity_type", entity_type,
                "--table_name", f"staging.staging_{entity_type}"
            ]
        )
 
    @task
    def upsert_entity(entity_type, ds=None):
        from airflow.sdk.bases.hook import BaseHook
        from src.transformations.runners.sql_runner import run_sql_file
        
        conn = BaseHook.get_connection("postgres_conn")
        conn_params = {
            "host": conn.host,
            "dbname": conn.schema,
            "user": conn.login,
            "password": conn.password,
            "port": conn.port
        }
        
        run_sql_file(conn_params, f"src/transformations/sql/upsert_{entity_type}.sql")

    # Assign tasks
    ingest = ingest_data()
    prep_buckets = prepare_buckets()
    prep_tables = prepare_tables()
    
    proc_country = process_entity.override(task_id="process_countries")("countries")
    proc_location = process_entity.override(task_id="process_locations")("locations")
    proc_parameter = process_entity.override(task_id="process_parameters")("parameters")
    proc_sensor = process_entity.override(task_id="process_sensors")("sensors")
    proc_measure = process_entity.override(task_id="process_measurements")("measurements")

    ins_country = insert_entity.override(task_id="insert_countries")("country")
    ins_location = insert_entity.override(task_id="insert_locations")("location")
    ins_parameter = insert_entity.override(task_id="insert_parameters")("parameter")
    ins_sensor = insert_entity.override(task_id="insert_sensors")("sensor")
    ins_measure = insert_entity.override(task_id="insert_measurements")("measurement")

    ups_country = upsert_entity.override(task_id="upsert_countries")("countries")
    ups_location = upsert_entity.override(task_id="upsert_locations")("locations")
    ups_parameter = upsert_entity.override(task_id="upsert_parameters")("parameters")
    ups_sensor = upsert_entity.override(task_id="upsert_sensors")("sensors")
    ups_measure = upsert_entity.override(task_id="upsert_measurements")("measurements")

    # Set up space
    prep_buckets >> prep_tables >> ingest
    
    ingest >> [proc_country, proc_location, proc_parameter, proc_sensor, proc_measure]

    proc_country >> ins_country
    proc_location >> ins_location
    proc_parameter >> ins_parameter
    proc_sensor >> ins_sensor
    proc_measure >> ins_measure

    [
        ins_country,
        ins_location,
        ins_parameter,
        ins_sensor,
        ins_measure
    ] >> ups_country

    ups_country >> ups_location >> ups_parameter >> ups_sensor >> ups_measure

openaq_pipeline()