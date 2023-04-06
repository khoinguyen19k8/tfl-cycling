from datetime import datetime
from airflow.decorators import dag
from groups.group_load_snowflake import group_load_snowflake
from airflow.sensors.external_task import ExternalTaskSensor


@dag(
    schedule="@monthly",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    template_searchpath="/opt/airflow/plugins/sql/snowflake",
)
def pq_to_snowflake():
    """
    Extract raw parquet data into snowflake.
    """
    raw_parquet_sensor = ExternalTaskSensor(
        task_id="raw_parquet_sensor",
        external_dag_id="incremental_extract_s3_to_pq",
        external_task_id="csv_to_pq",
    )

    raw_parquet_sensor >> group_load_snowflake()


pq_to_snowflake()
