from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


@task_group(group_id="group_load_snowflake")
def group_load_snowflake():
    create_stage = SnowflakeOperator(
        task_id="create_s3_stage",
        snowflake_conn_id="snowflake_tfl_cycling",
        sql="create_s3_stage.sql",
    )
    create_table = SnowflakeOperator(
        task_id="create_table",
        snowflake_conn_id="snowflake_tfl_cycling",
        sql="create_tfl_cycling_table.sql",
    )
    create_stage >> create_table
