# Architecture

## System context

```mermaid
flowchart LR
    ENG["Data engineer"]
    AIRFLOW["Airflow orchestration platform"]
    DBX["Databricks data platform"]
    CONSUMER["Analysts / downstream consumers"]

    ENG -->|"configures and triggers"| AIRFLOW
    AIRFLOW -->|"runs ingestion job"| DBX
    AIRFLOW -->|"executes dbt CLI"| DBX
    DBX -->|"serves transformed tables"| CONSUMER
    ENG -->|"monitors jobs and models"| DBX
```

## Local Airflow deployment

```mermaid
flowchart TB
    USER["Browser / CLI"] --> API["Airflow API Server<br/>port 8080"]

    subgraph COMPOSE["Docker Compose"]
        API
        SCHED["Scheduler"]
        DAGP["DAG Processor"]
        WORKER["Celery Worker"]
        TRIGGER["Triggerer"]
        REDIS[("Redis 7.2<br/>task broker")]
        POSTGRES[("PostgreSQL 16<br/>metadata + result backend")]

        SCHED --> REDIS --> WORKER
        SCHED --> POSTGRES
        API --> POSTGRES
        DAGP --> POSTGRES
        WORKER --> POSTGRES
        TRIGGER --> POSTGRES
    end

    DAGVOL["./dags"] -. "bind mount" .-> DAGP
    DBTVOL["./dbt_databricks"] -. "bind mount" .-> WORKER
    LOGVOL["./logs"] -. "bind mount" .-> WORKER
    CFGVOL["./config"] -. "bind mount" .-> API

    WORKER -->|"dbt commands"| SQL["Databricks SQL Warehouse"]
    TRIGGER -->|"poll run status"| JOBS["Databricks Jobs API"]
```

## Orchestration DAG

```mermaid
flowchart TB
    START(("Manual trigger")) --> INGEST["amazon_data_ingestion<br/>DatabricksRunNowOperator"]
    INGEST --> CLEAN["clean_target<br/>remove compiled artifacts and dbt logs"]
    CLEAN --> FRESH["source_freshness<br/>dbt source freshness"]
    FRESH --> STG["amazon_staging_dbt"]

    STG --> PRICE["amazon_price_info_dbt"]
    STG --> PRODUCT["amazon_product_info_dbt"]
    STG --> RATING["amazon_rating_info_dbt"]
    STG --> REVIEW["amazon_review_info_dbt"]

    PRICE --> EPH["eph_amazon_price_info_dbt<br/>compiles inline; no relation"]
    EPH --> LEAST["amazon_least_discounted_products_dbt"]
    EPH --> MOST["amazon_most_discounted_products_info_dbt"]

    PRODUCT --> DONE(("Complete"))
    RATING --> DONE
    REVIEW --> DONE
    LEAST --> DONE
    MOST --> DONE
```

The DAG has no schedule and no catch-up. Each run starts manually or through the Airflow API/CLI.

## End-to-end execution sequence

```mermaid
sequenceDiagram
    autonumber
    actor U as Operator
    participant A as Airflow Scheduler
    participant W as Celery Worker
    participant T as Airflow Triggerer
    participant J as Databricks Job
    participant S as Databricks SQL Warehouse

    U->>A: Trigger orchestrate DAG
    A->>W: Start DatabricksRunNowOperator
    W->>J: Submit existing ingestion job
    W-->>T: Defer task and release worker slot
    loop Every 30 seconds
        T->>J: Poll run state
        J-->>T: Current state
    end
    J-->>T: Success
    T-->>A: Resume task
    A->>W: Clean local dbt artifacts
    A->>W: Run source freshness
    W->>S: Query registered source
    A->>W: Run staging model
    W->>S: Create staging table
    par Independent model branches
        A->>W: Build product model
        A->>W: Build rating model
        A->>W: Build review model
        A->>W: Build price and discount models
    end
    W->>S: Materialize dbt tables
    A-->>U: DAG succeeds or exposes failed task
```

## Responsibility boundaries

| Component | Responsibility |
|---|---|
| Databricks ingestion job | Acquires and loads the Amazon source table; its implementation is external to this repository |
| Airflow DAG | Coordinates ingestion, cleanup, source check, and model execution |
| Triggerer | Polls the deferrable Databricks run without occupying a worker slot |
| Celery worker | Runs dbt CLI commands and non-deferred task work |
| Redis | Celery message broker |
| PostgreSQL | Airflow metadata database and Celery result backend |
| dbt | Resolves SQL model lineage, compiles Jinja, materializes relations, and executes tests |
| Databricks SQL warehouse | Executes dbt SQL against the configured catalog and schema |

## Failure behavior

- A failed Databricks ingestion run prevents all dbt work.
- A failed cleanup or source-freshness command prevents model execution.
- A failed staging model blocks every downstream model.
- Product, rating, review, and price branches run independently after staging; a failure in one does not prevent already-runnable sibling branches from executing.
- Failure of `amazon_price_info` blocks the ephemeral price transform and both discount-segment tables.

## Recommended production evolution

```mermaid
flowchart LR
    CURRENT["Local Compose<br/>static profile + job ID"]
    SECRETS["Secrets backend<br/>environment parameters"]
    DEPLOY["Versioned deployment<br/>CI/CD"]
    OBS["Remote logs<br/>metrics + alerts"]
    QUALITY["dbt build<br/>freshness SLAs + tests"]

    CURRENT --> SECRETS --> DEPLOY --> OBS --> QUALITY
```
