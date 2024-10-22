from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/arnab/WorkSpace/integrated-hawk-433114-s2-f604d4f40e6f.json"

#### DAG with create, and delete cluster ####
#cluster_name = "cluster-8399-testboto"
region = "us-central1"
project_id = "integrated-hawk-433114-s2"
bucket_name = 'spark_jobs_test_boto'
cluster_name = 'cluster-for-test-boto'
pySpark_clean_URI = "gs://spark_jobs_test_boto/script/data_clean_NYC.py"
pySpark_insert_URI = "gs://spark_jobs_test_boto/script/data_Ingest2bq_NYC.py"

cluster_config = ClusterGenerator(
    project_id=project_id,
    zone="us-central1-a",
    master_machine_type="e2-standard-2",
    worker_machine_type="e2-standard-2",
    num_workers=2,
    worker_disk_size=50,
    master_disk_size=100,
    storage_bucket=bucket_name,
).make()

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

dag = DAG('fetch_NYC_data_v2',
          default_args=default_args,
          description='Runs an external Python script to get Taxi data',
          schedule_interval='@daily',
          catchup=False)


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

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )

    run_script_task = BashOperator(
        task_id='run_script',
        bash_command='python3 /home/arnab/airflow/dags/data_extract_NYC.py',
    )
    
    create_cluster_task = DataprocCreateClusterOperator(
        task_id="create_cluster_task",
        project_id=project_id,
        cluster_config=cluster_config,
        region=region,
        cluster_name=cluster_name,
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

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=project_id, 
        cluster_name=cluster_name, 
        region=region,
    )

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
    )

# dependencies

start_pipeline >> run_script_task >> create_cluster_task >> clean_data_task >> insert_data_task >> delete_cluster >> finish_pipeline