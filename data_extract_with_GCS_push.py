import requests
import csv
from google.cloud import storage

url = 'https://cricbuzz-cricket.p.rapidapi.com/stats/v1/rankings/batsmen'
headers = {
        "X-RapidAPI-Key": "6cfe4e97b4mshd58ed2f55465541p16201ejsncf554f61995c",  # Replace with your RapidAPI key
    'X-RapidAPI-Host': 'cricbuzz-cricket.p.rapidapi.com'
}
params = {
    'formatType': 'odi'
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json().get('rank', [])
    csv_filename = 'batsmen_rankings.csv'

    if data:
        field_names = ['rank', 'name', 'country']


        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=field_names)
            # writer.writeheader()
            for entry in data:
                writer.writerow({field: entry.get(field) for field in field_names})

        print(f"Data fetched successfully and written to '{csv_filename}'")

        # Upload the CSV file to GCS
        bucket_name = 'testdatafromcricbizz'
        storage_client = storage.Client.from_service_account_json("/home/arnab/WorkSpace/integrated-hawk-433114-s2-f604d4f40e6f.json")
        bucket = storage_client.bucket(bucket_name)
        destination_blob_name = f'{csv_filename}'  # The path to store in GCS

        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(csv_filename)

        print(f"File {csv_filename} uploaded to GCS bucket {bucket_name} as {destination_blob_name}")
    else:
        print("No data available from the API.")
else:
    print("Failed to fetch data:", response.status_code)