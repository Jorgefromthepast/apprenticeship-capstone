#
# This DAG creates a pipeline for ETL, it starts with CSV files and the final goal is to get 

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)

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

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id="{{ var.value.project_id }}",
        cluster_config=Variable.get("cluster_config", deserialize_json = True),
        region="{{ var.value.region }}",
        cluster_name="{{ var.value.cluster_name }}"
    )

    pyspark_job_log_reviews = DataprocSubmitJobOperator(
        task_id="pyspark_job_log_reviews",
        project_id="{{ var.value.project_id }}",
        region="{{ var.value.region }}",
        job={
            "reference": {"project_id": "{{ var.value.project_id }}"},
            "placement": {"cluster_name": "{{ var.value.cluster_name }}"},
            "pyspark_job": {"main_python_file_uri": "{{ var.value.log_reviews_uri }}"},
            }        
    )

    pyspark_job_classification = DataprocSubmitJobOperator(
        task_id="pyspark_job_classification",
        project_id="{{ var.value.project_id }}",
        region="{{ var.value.region }}",
        job={
            "reference": {"project_id": "{{ var.value.project_id }}"},
            "placement": {"cluster_name": "{{ var.value.cluster_name }}"},
            "pyspark_job": {"main_python_file_uri": "{{ var.value.classification_uri }}"},
            }        
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.project_id }}",
        region="{{ var.value.region }}",
        cluster_name="{{ var.value.cluster_name }}",
        trigger_rule='all_done'
    )

    create_table >> import_csv >> dump_table >> create_cluster >> pyspark_job_log_reviews >> pyspark_job_classification >> delete_cluster