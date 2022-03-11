from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator
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
    dag_id='create_views',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['test']
    ) as dag:

    create_view_os = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id="{{ var.value.dataset_id }}",
        table_id="test_view",
        view=Variable.get("query_os_view", deserialize_json = True)
    )

    create_view_os