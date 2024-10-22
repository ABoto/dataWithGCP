from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# Initialize a Spark session
spark = SparkSession.builder \
.appName("CSV to BigQuery") \
.getOrCreate()

bucket_name = "spark_jobs_test_boto"
input_file_path = "output/cleaned_yellow_tripdata"
#gs://spark_jobs_test_boto/output/cleaned_yellow_tripdata_2024-01.csv/part-00000-5e31e5dd-d048-46a1-8de3-1126b4266b07-c000.csv
#csv_path = "gs://spark_jobs_test_boto/output/cleaned_yellow_tripdata_2024-01.csv/part-00000-ead35ddc-6e75-4fc3-bdfa-edff836eaca3-c000.csv"
#df = spark.read.csv(csv_path, header=True, inferSchema=True)
df = spark.read.format("csv").option("header", "true") \
.option("inferSchema", "true") \
.load(f"gs://{bucket_name}/{input_file_path}")
 
#region = "us-central1"
#bq_project_id = "integrated-hawk-433114-s2"
#bq_dataset_id = "integrated-hawk-433114-s2.TestBoto"
bq_table_id = "integrated-hawk-433114-s2.TestBoto.TestData"
df.write.format("bigquery").option('table', bq_table_id).option('temporaryGcsBucket',"gs://df_metadata_boto_test/temp").mode("overwrite").save()

spark.stop()