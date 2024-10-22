from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/arnab/WorkSpace/integrated-hawk-433114-s2-f604d4f40e6f.json"


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 21),
    'depends_on_past': False,
    'email': ['admin@admin.admin'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('fetch_NYC_data',
          default_args=default_args,
          description='Runs an external Python script to get Taxi data',
          schedule_interval='@daily',
          catchup=False)

# Dataproc cluster details
cluster_name = "cluster-8399-testboto"
region = "us-central1"
project_id = "integrated-hawk-433114-s2"
pySpark_clean_URI = "gs://spark_jobs_test_boto/script/data_clean_NYC.py"
pySpark_insert_URI = "gs://spark_jobs_test_boto/script/data_Ingest2bq_NYC.py"

pySpark_job_clean = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": pySpark_clean_URI},
}

pySpark_job_insert = {
    "reference": {"project_id": project_id},
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": pySpark_insert_URI},
}

with dag:
    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python3 /home/arnab/airflow/dags/data_extract_NYC.py',
    )
    
    clean_data_task = DataprocSubmitJobOperator(
        task_id="clean_data_task", 
        job=pySpark_job_clean, 
        region=region,
        project_id=project_id
    )

    insert_data_task = DataprocSubmitJobOperator(
        task_id="insert_data_task", 
        job=pySpark_job_insert, 
        region=region,
        project_id=project_id
    )

# Set the dependencies between the tasks

run_script_task >> clean_data_task >> insert_data_task