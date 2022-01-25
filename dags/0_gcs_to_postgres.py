# 
# Airflow DAG to create a table on a Postgres database and import a CSV file from GCS.

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

with DAG(
    dag_id='import_csv',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['capstone']
    ) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_default",
        sql='sql/create_table.sql'
    )

    import_csv = CloudSQLImportInstanceOperator(
        task_id='import_csv',
        gcp_conn_id='google_cloud_default',
        project_id="{{ var.value.project_id }}",
        body="{{ var.value.import_body }}",
        instance="{{ var.value.instance }}", 
    )

    create_table >> import_csv