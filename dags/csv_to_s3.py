from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator, S3CopyObjectOperator
from regex_engine import generator
import boto3
import re
from datetime import datetime
from pathlib import Path

def _extract_uris(ti):
    """
    Extract appropriate S3 URIs from a buckets. Filter only needed csv objects.
    """
    raw_uris = ti.xcom_pull(task_ids = "list_keys") 
    raw_uris = list(filter(None, [u.replace('usage-stats/', '').replace(' ', '') for u in raw_uris]))
    generate = generator()

    regex = generator().numerical_range(14, 352) # CSV data before 14 are unreliable.
    p = re.compile(regex[:-1] + "JourneyDataExtract")
    filtered_names_list = [f for f in raw_uris if (p.match(f) and f.endswith(".csv"))]
    
    ti.xcom_push(key="uris", value=filtered_names_list[:8])

def _log_xcom(ti):
    print(ti.xcom_pull(key="uris", task_ids="extract_uris"))

def _download_s3_local(**kwargs):
    """
    Download S3 objects from a bucket to a local directory.
    """
    BUCKET_NAME = kwargs["BUCKET_NAME"]
    BUCKET_PREFIX = kwargs["BUCKET_PREFIX"]
    DATA_DIR_PREFIX = kwargs["DATA_DIR_PREFIX"]
    ti = kwargs["ti"]
    uris_names = ti.xcom_pull(key="uris", task_ids="extract_uris")
    s3_hook = S3Hook(aws_conn_id="aws_delvo1919")
    
    Path(f"{DATA_DIR_PREFIX}").mkdir(parents=True, exist_ok=True)
    for uri in uris_names:
        s3_hook.download_file(key = f"{BUCKET_PREFIX}/{uri}", bucket_name = BUCKET_NAME, local_path = f"{DATA_DIR_PREFIX}")


def _upload_local_s3(**kwargs):
    BUCKET_NAME = kwargs["BUCKET_NAME"]
    DATA_DIR_PREFIX = kwargs["DATA_DIR_PREFIX"]
    BUCKET_KEY_PREFIX = kwargs["BUCKET_KEY_PREFIX"]
    ti = kwargs["ti"]
    file_paths = ti.xcom_pull(key="local_file_paths", task_ids="download_s3_local")
    s3_hook = S3Hook(aws_conn_id="aws_delvo1919")
    for f_path un file_paths:
        s3_hook.load_file(filename = f"{f_path}", key = f"{BUCKET_KEY_PREFIX}/{key}", bucket_name = f"{BUCKET_NAME}", replace = True)

def _copy_s3_to_s3(kwargs):
    SOURCE_BUCKET = kwargs["source_bucket"]
    DEST_BUCKET = kwargs["dest_bucket"]
    SOURCE_KEY_PREFIX = kwargs["source_key_prefix"]
    DEST_KEY_PREFIX = kwargs["dest_key_prefix"]
    ti = kwargs["ti"]
    uris_names = ti.xcom_pull(key="uris", task_ids="extract_uris")
    for uri in uris_names:
        s3_hook.download_file(key = f"{BUCKET_PREFIX}/{uri}", bucket_name = BUCKET_NAME, local_path = f"{DATA_DIR_PREFIX}")

with DAG("csv_to_s3", start_date=datetime(2023, 1, 1), catchup = False) as dag:

    list_keys = S3ListOperator(
        task_id="list_keys",
        bucket="cycling.data.tfl.gov.uk",
        prefix="usage-stats",
        aws_conn_id = "aws_delvo1919"
    )

    extract_uris = PythonOperator(
        task_id = "extract_uris",
        python_callable = _extract_uris
    )
    
    download_s3_local = PythonOperator(
        task_id = "download_s3_local",
        python_callable = _download_s3_local,
        op_kwargs = {"BUCKET_NAME" : 'cycling.data.tfl.gov.uk', "BUCKET_PREFIX" : 'usage-stats', "DATA_DIR_PREFIX" : '/opt/airflow/data'}
    )
    
    upload_local_s3 = PythonOperator(
        task_id = "upload_local_s3",
        python_callable = _upload_local_s3,
        op_kwargs = {"BUCKET_NAME": "tfl-cycling", "DATA_DIR_PREFIX": "/opt/airflow/data", "BUCKET_KEY_PREFIX": "raw"}
    )
    
    log_xcom = PythonOperator(
        task_id = "log_xcom",
        python_callable = _log_xcom
    )
    
    # s3_to_s3 = 
    
    list_keys >> extract_uris >> log_xcom
    extract_uris >> download_s3_local >> upload_local_s3



