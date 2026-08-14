# Security and operations

## Immediate repository hygiene

The repository currently tracks a root `.env`, Airflow runtime logs, and generated Python bytecode. These files should not be committed.

1. Treat any credential in the tracked `.env` or logs as potentially exposed.
2. Rotate affected Databricks tokens, Airflow secrets, and other credentials.
3. Remove sensitive files from the current tree and Git history where appropriate.
4. Commit the provided `.gitignore` and `.env.example`.
5. Review the repository's Git history and GitHub secret-scanning alerts before making it public or sharing it.

Deleting a file in a new commit does not remove it from earlier commits. Use an appropriate history-rewrite tool only after coordinating with collaborators because rewritten history requires everyone to re-clone or reset their branches.

## Secret placement

| Secret or setting | Recommended location |
|---|---|
| Databricks host and token | Airflow connection backed by a secrets backend |
| dbt Databricks credentials | Environment variables or generated profile outside Git |
| Airflow Fernet key | Secret manager or protected deployment secret |
| Airflow JWT secret | Secret manager or protected deployment secret |
| Databricks job ID | Airflow Variable or environment-specific configuration |
| Catalog and schema | dbt environment variables or target-specific profiles |

## Logs

Local logs are useful during development but can contain query text, connection metadata, job identifiers, stack traces, and occasionally values returned by commands. Keep `logs/` untracked. Production environments should use remote log storage with access control, retention rules, and encryption.

## Operational checks

Before triggering the DAG:

- Confirm every Compose service is healthy.
- Run `dbt debug` from the worker container.
- Confirm the Airflow connection `databricks_amazon_jobs` exists.
- Confirm the configured Databricks job ID belongs to the target workspace.
- Confirm the SQL warehouse is running or can auto-start.
- Confirm the service principal or user can run the job, read the source, and create target tables.

After a run:

- Verify the Databricks ingestion task completed successfully.
- Inspect source freshness output.
- Review failed and skipped tasks in Airflow Grid view.
- Run `dbt test` or incorporate tests into the DAG.
- Compare source and model row counts and investigate unexpected exclusions.

## Suggested environment strategy

| Environment | Catalog/schema example | Credentials | Airflow deployment |
|---|---|---|---|
| Development | `analytics_dev.dbt_amazon_<user>` | Individual or short-lived dev identity | Local Docker Compose |
| Test | `analytics_test.dbt_amazon` | CI service principal | CI runner or shared test Airflow |
| Production | `analytics_prod.dbt_amazon` | Production service principal | Managed or hardened Airflow |

## Reliability improvements

- Add retries, retry delays, task timeouts, and DAG-level failure notifications.
- Make the DAG idempotent and verify the external ingestion job's write behavior.
- Replace the destructive shell cleanup with `dbt clean` where possible.
- Use a pinned, tested dependency set instead of broad lower-bound-only versions.
- Run data tests in the orchestration path before downstream consumers are notified.
- Configure source freshness thresholds and loaded timestamps.
- Add SLAs or asset-based scheduling when the source delivery pattern is known.
