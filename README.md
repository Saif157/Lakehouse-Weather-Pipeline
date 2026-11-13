# Lakehouse-Weather-Pipeline
Implement a complete automated batch ETL pipeline for your own workflow using Apache Airflow, Databricks, and AWS.

## Project Architecture & Implementation

- **Apache Airflow — Workflow Orchestration:** Three decoupled DAGs manage the workflow lifecycle across daily incremental ingestion, chunked historical backfills, and dataset-triggered analytics. Parallel city extractions run via dynamic task mapping with exponential backoff retries and concurrency pools to honor API rate limits. A validation gate inspects landed S3 payloads to block downstream compute clusters if extraction fails.
  
- **Databricks — Data Processing:** Four PySpark notebooks implement the Medallion architecture, starting with Auto Loader streaming raw JSON into Bronze Delta tables. The Silver layer unpacks nested arrays, routes corrupt records to a quarantine table, and uses idempotent Delta MERGE operations to prevent duplicates. The Gold layer computes rolling weather features, exports partitioned Parquet, and enriches Brazilian Olist e-commerce orders to correlate delivery delays with rainfall.
  
- **AWS — Storage, Cataloging & Analytics:** Amazon S3 provides the decoupled storage tier housing immutable raw JSON envelopes, Delta Lake files, and Gold Parquet datasets. The AWS Glue Data Catalog maintains centralized table schemas and partition metadata across the pipeline. Amazon Athena executes serverless SQL queries over S3 without idle compute costs, while AWS CloudFormation provisions the infrastructure alongside FinOps query scan limits.

- **Athena SQL & Analytical Queries:** DDL scripts configure external tables with Partition Projection to eliminate slow metadata scans like MSCK REPAIR TABLE. Analytical queries evaluate logistics KPIs, identifying transit velocity drops and delivery SLA breach odds under severe weather conditions. Pre-aggregated CTAS queries build monthly rollup tables to accelerate BI dashboard reporting while reducing scanned bytes.
