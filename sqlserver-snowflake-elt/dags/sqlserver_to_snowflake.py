from airflow import DAG
from pendulum import datetime
from astro.sql import Table, merge
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook


def generate_create_table_sql(table):
    odbc_hook = OdbcHook(odbc_conn_id='mssql_default')
    records = odbc_hook.get_records(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
    columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
    return f"CREATE TABLE {table} ({columns})"


tables = ["table1", "table2", "table3"]

with DAG('mssql_to_snowflake',
         start_date=datetime(2024, 3, 18),
         schedule_interval=None,
         catchup=False) as dag:
    for table in tables:
        create_table = SnowflakeOperator(
            task_id=f"create_{table}_in_snowflake",
            snowflake_conn_id='snowflake_default',
            sql=generate_create_table_sql(table=table),
        )
    source_table = Table(name=table, conn_id='mssql_default')
    destination_table = Table(name=table, conn_id='snowflake_default')

    upload = merge(source_table, destination_table)
