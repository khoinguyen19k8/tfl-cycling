CREATE OR REPLACE FILE FORMAT tfl_cycling_parquet_format
    TYPE = parquet;

CREATE OR REPLACE STAGE tfl_cycling_s3_stage
    STORAGE_INTEGRATION = snowflake_integration
    URL = 's3://{{ dag_run.conf["parquet_bucket"] }}/{{ dag_run.conf["parquet_prefix"] }}'
    FILE_FORMAT = tfl_cycling_parquet_format;