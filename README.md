# Lakehouse-Weather-Pipeline
A complete automated batch ETL pipeline using Apache Airflow, Databricks, and AWS. Three Airflow DAGs orchestrate REST API ingestion, historical backfills, and Athena analytics. Databricks processes raw JSON through Bronze, Silver, and Gold layers, publishing analytical Parquet data to Amazon S3 for querying with Athena.
