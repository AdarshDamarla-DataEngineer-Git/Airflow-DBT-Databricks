# Project showcase

## One-line summary

Orchestrated a Databricks ingestion and dbt analytics pipeline with Apache Airflow 3.3, using deferrable remote-job execution, parallel model branches, data tests, and a containerized CeleryExecutor stack.

## Portfolio description

This project integrates Apache Airflow, dbt, and Databricks to process an Amazon product dataset. Airflow first triggers an external Databricks ingestion job, waits asynchronously through a deferrable operator, checks the dbt source, and builds a dependency-aware transformation graph. dbt separates product, price, rating, and review data and derives high- and low-discount product segments. The orchestration platform runs locally in Docker Compose with PostgreSQL, Redis, CeleryExecutor, a Triggerer, and dedicated scheduler, DAG processor, API server, and worker services.

## Resume bullets

- Built a containerized Apache Airflow 3.3 orchestration environment with CeleryExecutor, Redis, PostgreSQL, a dedicated Triggerer, and Docker Compose.
- Integrated Airflow with Databricks Jobs using a deferrable operator that releases worker capacity while remotely polling ingestion status.
- Designed a dbt-Databricks model graph for Amazon product, pricing, rating, review, and discount-segmentation analytics.
- Implemented dependency-aware parallel task execution, source checks, and dbt data tests to improve pipeline observability and data quality.

## Technologies demonstrated

```text
Apache Airflow 3
CeleryExecutor
Docker Compose
PostgreSQL
Redis
Databricks Jobs
Databricks SQL Warehouse
dbt Core
dbt-databricks
SQL / Jinja
```

## Interview talking points

### Why use a deferrable Databricks operator?

Remote jobs can run for a long time. Deferral moves polling to the Airflow Triggerer so the Celery worker slot is available for other work.

### Where does parallelism occur?

After staging completes, product, rating, review, and price transformations can run independently. The price branch then builds the ephemeral parser before producing the two discount segments.

### Why is the dbt model ephemeral?

The percentage-parsing logic is small and used as an intermediate transformation. Ephemeral materialization compiles it into downstream SQL without creating an additional warehouse relation.

### What would you improve next?

Move credentials to a secrets backend, parameterize workspace-specific values, add freshness SLAs and dbt tests to the DAG, pin dependencies, configure remote logging and alerts, and deploy through CI/CD.

## Suggested GitHub topics

```text
apache-airflow
airflow-3
dbt
dbt-databricks
databricks
data-engineering
analytics-engineering
workflow-orchestration
docker-compose
celery
postgresql
amazon-dataset
```

## Suggested repository description

Airflow 3 orchestrates Databricks ingestion and a dbt-Databricks analytics graph for Amazon product, price, rating, review, and discount data.
