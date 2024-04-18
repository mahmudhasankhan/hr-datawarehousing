from airflow import DAG
from pendulum import datetime
# from astro import sql as aql
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator


def generate_create_table_sql(table):
    odbc_hook = OdbcHook(odbc_conn_id='mssql_default')
    records = odbc_hook.get_records(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
    columns = ', '.join([f"{row[0]} {row[1]}" for row in records])
    columns = columns.replace("datetime2", "datetime")
    columns = columns.replace("ntext", "nvarchar")
    return f"CREATE TABLE IF NOT EXISTS {table} ({columns})"


def _transform_values(ti, table):
    data = ti.xcom_pull(key="return_value", task_ids=f'extract_from_{table}')

    def print_iterator(it):
        s = ", ".join(it)
        return s

    d = map(lambda x: str(tuple(x)), data)
    return print_iterator(d)


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
        extract = MsSqlOperator(
            task_id=f"extract_from_{table}",
            mssql_conn_id="mssql_default",
            sql=f"SELECT TOP(10) serialNo, employeeID, authDateTime, authDate, authTime, direction, deviceName, deviceSerialNo, name, cardNo FROM {table}",
        )

        transform = PythonOperator(
            task_id=f"transform_values_for_table_{table}",
            python_callable=_transform_values,
            op_kwargs={'table': table}
        )
        load = SnowflakeOperator(
            task_id=f"load_{table}_into_snowflake",
            snowflake_conn_id="snowflake_default",
            sql=f"INSERT INTO {table} VALUES {transform.output}"
        )

    # upload = aql.merge(
    #     source_table=Table(name=table, conn_id='mssql_default'),
    #     target_table=Table(name=table, conn_id='snowflake_default'),
    #     columns=['serialNo', 'employeeID', 'authDateTime', 'authDate', 'authTime',
    #              'direction', 'deviceName', 'deviceSerialNo', 'name', 'cardNo'],
    #     target_conflict_columns=["authDateTime", "deviceSerialNo"],
    #     if_conflicts="ignore"
    # )
    create_table >> extract >> transform >> load
