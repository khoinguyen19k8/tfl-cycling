import logging
from datetime import datetime
from pathlib import Path
from typing import Dict
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import task, get_current_context

@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False)
def s3_to_s3():

    def _filter_keys(keys: list, from_datetime: datetime, to_datetime: datetime) -> list:
        ending_formats = set([".xlsx", ".csv"])
        filtered_keys = [k["Key"] for k in keys if Path(k["Key"]).suffix in ending_formats]
        return filtered_keys
        
    @task(retries=1)
    def copy_objects() -> int:
        context = get_current_context()
        dag_run = context["dag_run"]
        dagrun_conf = dag_run.conf
        src_bucket, src_prefix = dagrun_conf["src_bucket"], dagrun_conf["src_prefix"]
        dest_bucket, dest_prefix = dagrun_conf["dest_bucket"], dagrun_conf["dest_prefix"]
        hook = S3Hook(aws_conn_id='aws_default')
        keys = hook.list_keys(bucket_name=src_bucket, prefix=src_prefix, object_filter=_filter_keys)
        for key in keys:
            hook.copy_object(
                source_bucket_name=src_bucket,
                dest_bucket_name=dest_bucket,
                source_bucket_key=key,
                dest_bucket_key=key,
                acl_policy='bucket-owner-full-control'
            )
        return len(keys)

    copy_objects()

s3_to_s3()