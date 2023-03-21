import logging
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context
from groups.group_s3_to_s3 import s3_to_s3


@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def dag_tfl_cycling():
    pass
