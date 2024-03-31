from airflow.decorators import task, dag
from datetime import datetime, timedelta
import pyodbc
import os
from dotenv import load_dotenv, find_dotenv

default_args = {
    "start_date": datetime(2024, 3, 29)
}

_ = load_dotenv(find_dotenv())

SERVER = os.environ["SERVER"]
DATABASE = os.environ["DATABASE"]
USERNAME = os.environ["USER"]
PASSWORD = os.environ["PASSWORD"]


@dag(
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    concurrency=2,
    max_active_runs=1)
def my_dag():

    @task(task_id='testsql')
    def test_sql():
        # Create a connection string variable using string interpolation.
        connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;'
        # Use the pyodbc.connect function to connect to an SQL database.
        conn = pyodbc.connect(connectionString)
        SQL_QUERY = """
        SELECT TOP(100) *
        FROM
        ATTENDANCE
        ORDER BY authDateTime DESC
        """
        # Use cursor.execute to retrieve a result set from a query against the database.
        cursor = conn.cursor()
        for row in cursor.execute(SQL_QUERY):
            print(f"{row.serialNo} | {row.employeeID} | {row.authDateTime} | {row.authDate} | {row.authTime} | {row.deviceName} | {row.name}")

    test_sql()


sqltestdag = my_dag()
