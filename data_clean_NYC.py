from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from google.cloud import storage
from pyspark.sql.window import Window

bucket_name = "spark_jobs_test_boto"
input_file_path = "data/yellow_tripdata_2024-01.parquet"
output_file_path = "output/cleaned_yellow_tripdata"

spark = SparkSession.builder.getOrCreate()
df = spark.read.format("parquet").option("header", "true").load(f"gs://{bucket_name}/{input_file_path}")
df = df.dropna()
df = df.dropDuplicates()
df = df.filter(df["tolls_amount"] > 0)
#agg_df = df.groupBy(df.VendorID).agg(avg(df.total_amount))
#agg_df.show()
# df.createOrReplaceTempView("trip_table")
# result_df = spark.sql()
timeFormat = "yyyy-MM-dd HH:mm:ss"
timeDiff = (unix_timestamp('tpep_dropoff_datetime', format=timeFormat)
            - unix_timestamp('tpep_pickup_datetime', format=timeFormat))
df = df.withColumn("trip_duration", timeDiff)

windowPartitionAgg  = Window.partitionBy("trip_Duration")
df = df.withColumn("Avg_distance", avg(col("trip_distance")).over(windowPartitionAgg)) \
       .withColumn("total_distance", sum(col("trip_distance")).over(windowPartitionAgg)) \
       .withColumn("Min_distance",min(col("trip_distance")).over(windowPartitionAgg)) \
       .withColumn("Max_distance", max(col("trip_distance")).over(windowPartitionAgg))
       
df.write.format("csv").option("header", "true").mode("overwrite").save(f"gs://{bucket_name}/{output_file_path}")
#df.write.format("csv").option("header", "true").mode("overwrite").save(f"gs://{bucket_name}/{output_file_path}")
spark.stop()