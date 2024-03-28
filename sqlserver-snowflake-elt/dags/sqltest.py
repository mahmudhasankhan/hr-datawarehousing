from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
import time
from datetime import datetime, timedelta
import pyodbc
import os
from dotenv import load_dotenv, find_dotenv

default_args = {
    "start_date": datetime(2024, 3, 29)
}

_ = load_dotenv(find_dotenv())


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
        server = os.environ["SERVER"]
        username = os.environ["USER"]
        password = os.environ["PASSWORD"]
        database = os.environ["DATABASE"]
        ddw_connection = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server +
                                        ';DATABASE='+database+';UID='+username+';PWD=' + password+';TrustServerCertificate=yes;')
        query1 = "select top(10) from attendance"
        print(f"Running query: {query1}")
        print(f"Connection String: {ddw_connection}")
        cursor = ddw_connection.cursor()
        queryresult = cursor.execute(query1)
   # database_names    = [db.ddw_databasename for db in databases_to_sync]
        print(queryresult)

    test_sql()


sqltestdag = my_dag()
