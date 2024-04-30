from datetime import datetime

from airflow import models
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeSqlApiOperator

SNOWFLAKE_CONN_ID = "my_snowflake_conn"
SNOWFLAKE_SAMPLE_TABLE = "sample_table"
CREATE_TABLE_SQL_STRING = (
    f"CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_SAMPLE_TABLE} (name VARCHAR(250), id INT);"
)
SQL_INSERT_STATEMENT = f"INSERT INTO {SNOWFLAKE_SAMPLE_TABLE} VALUES ('name', %(id)s)"
SQL_LIST = [SQL_INSERT_STATEMENT % {"id": n} for n in range(10)]
SQL_MULTIPLE_STMTS = "; ".join(SQL_LIST)
DAG_ID = "example_snowflake"


with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    schedule=None,
    tags=["example", "adls", "snowflake", "ETL"],
) as dag:
    snowflake_op_sql_str = SnowflakeOperator(task_id="create_table_using_sql", sql=CREATE_TABLE_SQL_STRING)

    # # Upload data to ADLS
    # upload_data = ADLSCreateObjectOperator(
    #     task_id="upload_data_to_adls",
    #     file_system_name="Fabric",
    #     file_name=REMOTE_FILE_PATH,
    #     data="Hello world",
    #     replace=True,
    # )

    # Create a SnowflakeOperator task to insert a single row with parameters
    snowflake_op_with_params = SnowflakeOperator(
        task_id="insert_isingle_row",
        sql=SQL_INSERT_STATEMENT,
        parameters={"id": 56},
    )

    snowflake_op_sql_list = SnowflakeOperator(task_id="insert_rows_with_params", sql=SQL_LIST)

    # Task to execute multiple Snowflake SQL statements separated by semicolons
    snowflake_op_sql_multiple_stmts = SnowflakeOperator(
        task_id="snowflake_op_sql_multiple_stmts",
        sql=SQL_MULTIPLE_STMTS,
        split_statements=True,
    )

    snowflake_op_template_file = SnowflakeOperator(
        task_id="snowflake_op_template_file",
        sql="example_snowflake_snowflake_op_template_file.sql",
    )

    snowflake_sql_api_op_sql_multiple_stmt = SnowflakeSqlApiOperator(
        task_id="snowflake_op_sql_multiple_stmt",
        sql=SQL_MULTIPLE_STMTS,
        statement_count=len(SQL_LIST),
    )

    (
        snowflake_op_sql_str
        >> [
            snowflake_op_with_params,
            snowflake_op_sql_list,
            snowflake_op_template_file,
            snowflake_op_sql_multiple_stmts,
            snowflake_sql_api_op_sql_multiple_stmt,
        ]
    )