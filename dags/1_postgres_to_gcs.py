# 
# Airflow DAG to dump a PostgresDB table into a parquet file in GCS.

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

# Default arguments
default_args = {
    'owner': 'jorge.vega',
    'depends_on_past': False,    
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    dag_id='dump_table',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['capstone']
    ) as dag:

    dump_table = PostgresToGCSOperator(
        task_id='dump_table',
        postgres_conn_id = 'postgres_default',
        use_server_side_cursor = True,
        sql = '{{ var.value.query }}',
        bucket = '{{ var.value.staging_bucket }}',
        filename = '{{ var.value.filename_parquet }}',
        export_format='json',
        approx_max_file_size_bytes=1048576
    )

    dump_table