#
# This DAG creates a pipeline for ETL, it starts with CSV files and the final goal is to get 

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# Default arguments
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2021, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='pipeline',
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
        body=Variable.get('import_body', deserialize_json = True),
        instance="{{ var.value.instance }}", 
    )

    dump_table = PostgresToGCSOperator(
        task_id='dump_table',
        postgres_conn_id='postgres_default',
        use_server_side_cursor=True,
        sql='{{ var.value.query }}',
        bucket='{{ var.value.staging_bucket }}',
        filename='{{ var.value.filename_json }}',
        export_format='json'
    )

    create_table >> import_csv >> dump_table