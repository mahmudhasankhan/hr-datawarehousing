import logging
from airflow import DAG
from pendulum import datetime
from astro import sql as aql
from astro.sql.table import Table
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook


def generate_create_table_sql(table):
    odbc_hook = OdbcHook(odbc_conn_id='mssql_default')
    records = odbc_hook.get_records(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
    # logging.info(records)
    # columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
    # columns.replace("datetime2", "datetime")
    s = "CREATE TABLE attendance (serialNo int, employeeID nvarchar, authDateTime datetime, authDate nvarchar, authTime nvarchar, direction nvarchar, deviceName nvarchar, deviceSerialNo nvarchar, name nvarchar, cardNo nvarchar)"
    # return f"CREATE TABLE {table} ({columns})"
    return s


tables = ["attendance"]

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

    upload = aql.merge(
        source_table=Table(name=table, conn_id='mssql_default'),
        target_table=Table(name=table, conn_id='snowflake_default'),
        columns=['serialNo', 'employeeID', 'authDateTime', 'authDate', 'authTime',
                 'direction', 'deviceName', 'deviceSerialNo', 'name', 'cardNo'],
        target_conflict_columns=["serialNo"],
        if_conflicts="update"
    )
    create_table >> upload
