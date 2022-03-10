#
# Airflow DAG to create a Dataproc cluster, submit a Pyspark Job from GCS and
# destroy the cluster.

from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator
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

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource="{{ var.json.table_resource }}"
    )

    create_external_table