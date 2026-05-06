# OpenAQ-Data-Pipeline
An end-to-end data pipeline that ingests, processes, and loads air quality data from the OpenAQ API using Apache Airflow, Apache Spark, and MinIO.

## Overview
This project builds a **modular and scalable ETL pipeline** for air quality data. It demonstrates how to orchestrate workflows, process large datasets, and design reliable data pipelines using modern data engineering tools.

The pipeline follows a layered approach:

* **Bronze** → Raw data ingestion
* **Silver** → Cleaned and structured data
* **Gold** → Analytics-ready tables in PostgreSQL

## Tech Stack

* **Orchestration**: Apache Airflow
* **Processing**: Apache Spark (PySpark)
* **Storage**: MinIO (S3-compatible object storage)
* **Database**: PostgreSQL
* **Containerization**: Docker & Docker Compose
* **Data Source**: OpenAQ API

## Architecture
OpenAQ API
    ↓
Airflow (Orchestration)
    ↓
MinIO (Bronze - Raw JSON)
    ↓
Spark Jobs (Transformations)
    ↓
MinIO (Silver - Structured Data)
    ↓
PostgreSQL (Gold - Final Tables)
