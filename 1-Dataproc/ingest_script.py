from google.cloud import storage
from google.oauth2 import service_account
import os
import json
import pyarrow.parquet as pq
import pyarrow as pa

project_id = '<your_project_id>'

with open('<path/to/your_service_account.json>') as source:
    info = json.load(source)

storage_credentials = service_account.Credentials.from_service_account_info(info)

BUCKET = '<your_bucket_name>'
url_template = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'

table_schema_yellow = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64()),
        ('airport_fee', pa.float64())]

)

def format_to_parquet(src_file, service):

    table = pq.read_table(src_file)

    table = table.cast(table_schema_yellow)
    
    pq.write_table(table, src_file)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  storage_client = storage.Client(project=project_id, credentials=storage_credentials)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  print('File {} uploaded to {}.'.format(
      source_file_name,
      destination_blob_name))

def main(service, year):
    for i in range(13):
        if i != 12:
            month = '0'+str(i+1)
            month = month[-2:]
            file_name = service + '_tripdata_' + year + '-' + month + '.parquet'
            request_url = url_template + file_name
            os.system(f'wget {request_url} -O {file_name}')
            format_to_parquet(file_name)
            print(f"Parquet: {file_name}")
            upload_blob(BUCKET, file_name, f"{year}/{file_name}")
            print(f"GCS: {service}/{file_name}")
            os.system(f'rm {file_name}')

main('yellow', '2020')




