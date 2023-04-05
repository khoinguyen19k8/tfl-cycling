import logging
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from groups.group_s3_to_s3 import s3_to_s3


@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def initial_tfl_cycling():
    """
    Initial ETL to extract csv data from cycling.data.tfl.gov.uk, load into personal S3, then transform them into parquet files
    using databricks jobs.
    """

    clean_up_parquets = S3DeleteObjectsOperator(
        task_id="clean_up_parquets",
        bucket='{{ dag_run.conf["parquet_bucket"] }}',
        prefix='{{ dag_run.conf["parquet_prefix"] }}',
        aws_conn_id="aws_default",
    )

    csv_to_pq = DatabricksRunNowOperator(
        task_id="csv_to_pq",
        databricks_conn_id="databricks_default",
        job_id="612164541494714",
    )

    s3_to_s3() >> clean_up_parquets >> csv_to_pq


initial_tfl_cycling()
