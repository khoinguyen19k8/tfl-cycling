from datetime import datetime
from pathlib import Path
from airflow.decorators import dag
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator


@dag(schedule="@monthly", start_date=datetime(2023, 4, 1), catchup=False)
def incremental_extract_s3_to_pq():
    """
    Incremental ETL to sync csv data from cycling.data.tfl.gov.uk on a monthly basis, load into personal S3, then transform them into parquet files
    using databricks jobs.
    """

    s3_sync = BashOperator(
        task_id="s3_sync",
        bash_command="aws s3 sync {{ dag_run.conf['src_bucket'] }}/{{ dag_run.conf['src_prefix'] }}  {{ dag_run.conf['dest_bucket'] }}/{{ dag_run.conf['dest_prefix'] }} --exclude '*' --include '*.csv'",
    )
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

    s3_sync >> clean_up_parquets >> csv_to_pq


incremental_extract_s3_to_pq()
