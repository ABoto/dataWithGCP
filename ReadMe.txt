

Local unix server has been used to configure Airflow. This has been done to keep airflow independent. So in future we can use the DAGs and other scripts in it, on multiple platforms with some basic changes in the code. 
Three tasks running in airflow Run Script, Clean Data Task and Insert Data Task

Run Script - This will run a python script data_extract_NYC.py to get data downloaded from NYC taxi portal and transfer the data to gcs bucket inside gcp.

Clean Data Task - This will trigger a Dataproc job submission in a pre launched cluster. The task will call another pySpark code present in data_clean_NYC.py. The data will drop null value, drop duplicate values, filter out data where tolls_amount is greater than 0 and save the file as csv in another gcs bucket.



Insert Data Task - This will trigger a Dataproc job submission in the same cluster. The Task will take the csv file from gcs and insert the data in a bigquery table.

Reporting- Looker Studio has been integrated with the BigQuery Tablet to visualize the data.

https://lookerstudio.google.com/reporting/d865e11d-b448-4478-8c0c-9ebd25463853/page/jabGE

Steps
1. After installation of airflow, put dag_NYC.py in the dags folder.
Note: dag_NYC_v2 will create the Dataproc cluster and delete it after use. Free tier space limitation is there so having difficulty with defaut configurations. 
2. 
