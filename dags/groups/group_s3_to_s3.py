from datetime import datetime
from pathlib import Path
from airflow.decorators import task, task_group
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import get_current_context


def _filter_keys(keys: list, from_datetime: datetime, to_datetime: datetime) -> list:
    """
    Filter keys that are csv and Excel files.
    """
    ending_formats = set([".xlsx", ".csv"])
    filtered_keys = [k["Key"] for k in keys if Path(k["Key"]).suffix in ending_formats]
    return filtered_keys


@task(retries=1)
def extract_keys() -> list:
    """
    Extract objects that have been filtered.
    """
    context = get_current_context()
    dag_run = context["dag_run"]
    dagrun_conf = dag_run.conf
    src_bucket, src_prefix = dagrun_conf["src_bucket"], dagrun_conf["src_prefix"]
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(
        bucket_name=src_bucket, prefix=src_prefix, object_filter=_filter_keys
    )
    return keys


@task(retries=1)
def copy_object(key: str) -> None:
    """
    Copy all objects from src_bucket -> dest_bucket.
    """
    context = get_current_context()
    dag_run = context["dag_run"]
    dagrun_conf = dag_run.conf
    src_bucket = dagrun_conf["src_bucket"]
    dest_bucket, dest_prefix = dagrun_conf["dest_bucket"], dagrun_conf["dest_prefix"]

    hook = S3Hook(aws_conn_id="aws_default")
    hook.copy_object(
        source_bucket_name=src_bucket,
        dest_bucket_name=dest_bucket,
        source_bucket_key=key,
        dest_bucket_key=key,
        acl_policy="bucket-owner-full-control",
    )


@task()
def clean_excel() -> None:
    """
    Remove the excel file and copy the csv file in place of it.
    """
    context = get_current_context()
    dag_run = context["dag_run"]
    dagrun_conf = dag_run.conf
    dest_bucket, dest_prefix = dagrun_conf["dest_bucket"], dagrun_conf["dest_prefix"]
    hook = S3Hook(aws_conn_id="aws_default")
    hook.copy_object(
        source_bucket_name=dest_bucket,
        dest_bucket_name=dest_bucket,
        source_bucket_key=f"excel_replacement/49JourneyDataExtract15Mar2017-21Mar2017.csv",
        dest_bucket_key=f"{dest_prefix}/49JourneyDataExtract15Mar2017-21Mar2017.csv",
    )
    hook.delete_objects(
        bucket=dest_bucket,
        keys=f"{dest_prefix}/49JourneyDataExtract15Mar2017-21Mar2017.xlsx",
    )


@task_group(group_id="s3_to_s3")
def s3_to_s3():
    copy_object.expand(key=extract_keys()) >> clean_excel()
