#
# Airflow DAG to create a Dataproc cluster, submit a Pyspark Job from GCS and
# destroy the cluster.

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
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
    dag_id='submit_jobs',
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
    tags=['test']
    ) as dag:

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

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id="{{ var.value.project_id }}",
        region="{{ var.value.region }}",
        cluster_name="{{ var.value.cluster_name }}"
    )

    create_cluster >> pyspark_job_log_reviews >> delete_cluster