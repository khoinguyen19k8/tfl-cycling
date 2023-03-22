import logging
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from groups.group_s3_to_s3 import s3_to_s3


@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def dag_tfl_cycling():
    # @task()
    # def clean_up_parquets():
    #     """
    #     Clean up parquet and other files from previous run to ensure idempotency of the pipeline.
    #     """
    #     pass

    csv_to_pq = DatabricksRunNowOperator(
        task_id="csv_to_pq",
        databricks_conn_id="databricks_default",
        job_id="612164541494714",
    )
