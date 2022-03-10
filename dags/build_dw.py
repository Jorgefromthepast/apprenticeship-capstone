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
    dag_id='build_dw',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test']
    ) as dag:

    create_ext_table_logs = BigQueryCreateExternalTableOperator(
        task_id="create_ext_table_logs",
        table_resource=Variable.get("table_resource_logs", deserialize_json = True)
    )

    create_ext_table_reviews = BigQueryCreateExternalTableOperator(
        task_id="create_ext_table_reviews",
        table_resource=Variable.get("table_resource_reviews", deserialize_json = True)
    )

    create_ext_table_user_purchase = BigQueryCreateExternalTableOperator(
        task_id="create_ext_table_user_purchase",
        table_resource=Variable.get("table_resource_user_purchase", deserialize_json = True)
    )

    [create_ext_table_logs, create_ext_table_reviews, create_ext_table_user_purchase]