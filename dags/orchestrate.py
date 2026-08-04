from airflow.sdk import dag, task
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


@dag(
    dag_id="orchestrate",
    start_date=datetime(2026, 8, 3),
    schedule=None,
    catchup=False,
)
def orchestrate():

    amazon_data_ingestion = DatabricksRunNowOperator(
        task_id="amazon_data_ingestion",
        databricks_conn_id="databricks_amazon_jobs",
        job_id=77822646485475,
        deferrable=True,          # releases worker slot, polls via Triggerer instead
        polling_period_seconds=30, # how often the Triggerer checks Databricks run status
    )

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/dbt_databricks/target && rm -rf /opt/airflow/dbt_databricks/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/dbt_databricks && dbt source freshness"

    amazon_staging_dbt = BashOperator(
        task_id='amazon_staging_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_staging'
    )

    amazon_price_info_dbt = BashOperator(
        task_id='amazon_price_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_price_info'
    )


    amazon_product_info_dbt = BashOperator(
        task_id='amazon_product_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_product_info'
    )

    amazon_rating_info_dbt = BashOperator(
        task_id='amazon_rating_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_rating_info'
    )

    amazon_review_info_dbt = BashOperator(
        task_id='amazon_review_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_review_info'
    )

    eph_amazon_price_info_dbt = BashOperator(
        task_id='eph_amazon_price_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select eph_amazon_price_info'
    )

    amazon_least_discounted_products_dbt = BashOperator(
        task_id='amazon_least_discounted_products_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_least_discounted_products'
    )

    amazon_most_discounted_products_info_dbt = BashOperator(
        task_id='amazon_most_discounted_products_info_dbt',
        cwd='/opt/airflow/dbt_databricks',
        bash_command='dbt run --select amazon_most_discounted_products'
    )

    
    amazon_data_ingestion >> clean_target() >> source_freshness() >> amazon_staging_dbt >> \
    [amazon_price_info_dbt, amazon_product_info_dbt, amazon_rating_info_dbt, amazon_review_info_dbt]

    amazon_price_info_dbt >> eph_amazon_price_info_dbt \
          >> [amazon_least_discounted_products_dbt, amazon_most_discounted_products_info_dbt]

orchestrate()