from google.cloud import storage

def upload_to_gcs(bucket_name, local_file_path, destination_blob_name):


    storage_client = storage.Client.from_service_account_json("/home/arnab/WorkSpace/integrated-hawk-433114-s2-f604d4f40e6f.json")
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(local_file_path)

    print(f"File {local_file_path} uploaded to {destination_blob_name}.")

bucket_name = "spark_jobs_test_boto"
local_file_path = "/home/arnab/WorkSpace/Data/yellow_tripdata_2024-01.parquet"
destination_blob_name = "data/yellow_tripdata_2024-01.parquet"

upload_to_gcs(bucket_name, local_file_path, destination_blob_name)