# HR Datawarehousing

## Background: 
As a data analyst, I have done Human Resource Data Warehousing with SQL Server and performed data visualization
with PowerBI. From data warehousing to data modeling everything was done with sql server and orchestration was handled with SSIS(SQL SERVER INTEGRATION SERVICES). 


Now I want to achieve the same thing but trying modern technologies like dbt (transformations), snowflake (data-warehouse) and airflow (orchestration).

## Project Idea:
- **E**xtract Data from source (sql server)
- **L**oad data from source to Snowflake (as staging tables), and 
- **T**ransform the loaded data into new dimensions models with dbt.

Lastly visualize the data model with PowerBI.

All of these jobs are to be orchestrated with airflow!

### Bonus: 
I have demonstrated here how to connect to an MSSQL database using Airflow.

A lot of goes under the hood in the dockerfile to establish a stable connection between MSSQL and Airflow.

In brief, installing odbc driver for sql server 18, and pyodbc in the docker image building process and 
then connecting a mssql database through pyodbc package with the help of odbc API does the trick.

## Project Setup:

### Setup SQL Server with Airflow

Prerequisites:
- sql server
- docker
- astro cli

Use astro cli to spin up an airflow environment with this command:
```
astro dev init
```
create a .env file and define your environment variables required for connecting SQL server with pyodbc 
```
# MSSQL env variables
SERVER=
USER=
PASSWORD=
DATABASE=

```
### Install necessary dependencies for pyodbc and mssql

Refer to this [dockerfile](https://github.com/mahmudhasankhan/hr-datawarehousing/blob/master/sqlserver-snowflake-elt/Dockerfile).


```docker
FROM quay.io/astronomer/astro-runtime:10.5.0-python-3.10

USER root

RUN curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc \
    && curl https://packages.microsoft.com/config/debian/11/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list \
    && echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections \
    && sudo apt-get install -y -q \
    && sudo apt-get update \
    && sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && sudo ACCEPT_EULA=Y apt-get install -y mssql-tools18 \
    && echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc \
    && source ~/.bashrc \
    && chmod +rwx /etc/ssl/openssl.cnf \
    && sed -i 's/TLSv1.2/TLSv1/g' /etc/ssl/openssl.cnf \
    && sed -i 's/SECLEVEL=2/SECLEVEL=1/g' /etc/ssl/openssl.cnf
USER airflow
```
These bash commands were curated from different sources like official documentations and mostly github issues and some stackoverflow questions etc. Yes, I haven't use chatgpt lol.


Basically, on steps where I have found obstacles, I have solved by reading through multiple documentations and github issues. I am listing down the resources down below.
- [Official Microsoft documentation on how to install odbc driver for linux](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver16&tabs=debian18-install%2Calpine17-install%2Cdebian8-install%2Credhat7-13-install%2Crhel7-offline#18)
- 

Once this docker image build is done and airflow webserver has succesfully started, docker exec into the webserver by
```docker
docker exec -u root -it <container id> bash
```
and verify by running 

### With Pyodbc:


### With airflow ODBC hook
Create an ODBC Connection in airflow

```
Connection id = mssql_default
Connection Type = ODBC
Host = server ip
Schema = dbo
Login = username
Password = ****
Port = 1433
Extra = {"Driver": "ODBC Driver 18 for SQL Server", "ApplicationIntent": "ReadOnly", "TrustedConnection": "Yes", "connect_kwargs": {"autocommit": false, "ansi": true}}
```
Snowflake Connections 
### Install dbt

You need to install dbt-core and dbt-snowflake adapter in your local machine.

Run these commands in your bash terminal.
```bash
pip install dbt-core \

&& pip install dbt-snowflake
```
### Setup Snowflake Environment

In snowflake we're gonna create a datawarehouse from where we'll visualize data later.
For datawarehousing, we will create a database and a role. We will also define a schema where we will put our dbt-tables (staging tables & data marts, fact tables) in.


**Snowflake warehouse:** A virtual warehouse, often referred to simply as a “warehouse”, is a cluster of compute resources in Snowflake.
A warehouse provides the required resources, such as CPU, memory, and temporary storage, to perform operations in a Snowflake session.

**Database & Schema**: All data in Snowflake is maintained in databases. Each database consists of one or more schemas, which are logical groupings of database objects, such as tables and views.

**Views**: A view allows the result of a query to be accessed as if it were a table. 

A view is a defined query that sits on top of a table. Unlike a table, it doesn't store the actual data. It always contains the latest data because it reruns every time it is queried. Whereas a table is only as fresh as the last time it was created or updated, no matter when you query it.

Run the following commands in your snowflake sql worksheet
```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE hr_wh WITH warehouse_size='x-small';

CREATE DATABASE IF NOT EXISTS hr_db;

CREATE ROLE IF NOT EXISTS hr_admin_role;

SHOW GRANTS ON WAREHOUSE hr_wh;


GRANT usage ON WAREHOUSE hr_wh TO ROLE hr_admin_role;

GRANT ROLE hr_admin_role TO USER mahmudhasan141;
    
GRANT ALL ON DATABASE hr_db TO ROLE hr_admin_role;

USE ROLE hr_admin_role;

CREATE SCHEMA hr_db.hr_schema;

-- to drop your database and warehouse
use role accountadmin;
drop IF EXISTS warehouse hr_wh;
drop IF EXISTS database hr_db;
drop role IF EXISTS hr_admin_role;
```


