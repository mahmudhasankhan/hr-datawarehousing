# HR Datawarehousing

## Project Desc: 
This project is a personal project. I have done HR data warehousing with sql server and visualized the 
date with PowerBI. Now I want to use modern tools such as dbt (transformations), snowflake (data-warehouse) and airflow (orchestration)

## Project Idea:
Extract Data from source server (sql server), load data in Snowflake (as staging tables), and transform into new data models with dbt, 

Lastly visualize the data model with PowerBI.

We have a lot of moving parts here and **airflow** is gonna help us hook everything together into one unit

## Project Setup:

### Setup SQL Server with Airflow

Prerequisites:
- sql server
- astro cli

Use astro cli to spin up an airflow environment. 
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


