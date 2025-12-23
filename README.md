# manual-beginner-pipeline
core concepts—Airflow orchestration, dbt modeling, and task dependencies—without unnecessary complexity

## Project Overview
This is a beginner-level manual data pipeline demonstrating how to orchestrate
Python ingestion and dbt transformations using Airflow.

## Tech Stack
- Python
- PostgreSQL
- dbt
- Apache Airflow

## Pipeline Flow
1. Load raw CSV data into PostgreSQL
2. Run dbt staging models
3. Run dbt fact models
4. Run dbt tests

## Notes
- The pipeline is manually triggered
- Dataset is static
- Focus is on orchestration and transformation structure
