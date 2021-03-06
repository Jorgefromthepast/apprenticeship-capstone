from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator

# Default arguments 
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

import_body = {"importContext": {
    "fileType": "csv",
    "uri": 'gs://gcp-data-eng-appr02-raw/user_purchase_no_headers.csv',
    "database": 'dbname',
    "csvImportOptions": {
    "table": 'apprenticeship.user_purchase',
        },
    "importUser": 'dbuser'
    }}

with DAG(
    dag_id='create_table',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['capstone']
    ) as dag:


    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS apprenticeship;
            CREATE TABLE IF NOT EXISTS apprenticeship.user_purchase (
            invoice_number VARCHAR(10),
            stock_code VARCHAR(20),
            detail VARCHAR(1000),
            quantity INT,
            invoice_date TIMESTAMP,
            unit_price NUMERIC(8,3),
            customer_id INT,
            country VARCHAR(20)
            );
          """,
    )

    sql_import_task = CloudSQLImportInstanceOperator(
        task_id='sql_import_task',
        project_id='gcp-data-eng-appr02-ba95f6e2',
        body=import_body,
        instance="{{ var.value.instance }}", 
    )

create_schema >> sql_import_task