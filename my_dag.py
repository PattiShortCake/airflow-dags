from airflow import DAG
from airflow.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago
from airflow.configuration import conf

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='deferrable_glue_job_kubernetes_executor',
    default_args=default_args,
    schedule_interval=None, 
)

glue_job_operator = GlueJobOperator(
    task_id='glue_job_task',
    job_name='csv-to-iceberg', 
    verbose=True,
    stop_job_run_on_kill=True
)

glue_job_operator 
