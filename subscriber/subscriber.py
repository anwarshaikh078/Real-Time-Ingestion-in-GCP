import base64
from google.cloud import bigquery
from google.cloud import storage
import os
import json
from datetime import date
from datetime import datetime
import time
    
def user_activity(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message_id = context.event_id
    dataset_id = os.environ.get('DATASET_ID', 'Specified environment variable is not set.')
    table_id = os.environ.get('TABLE_ID', 'Specified environment variable is not set.')
    bucket_name = os.environ.get('BUCKET_NAME', 'Specified environment variable is not set.')
    
    data = json.loads(pubsub_message)
    
    try:
         data = [data]
         client_b=bigquery.Client()
         table_ref = client_b.dataset(dataset_id).table(table_id)
         table = client_b.get_table(table_ref)
         errors = client_b.insert_rows_json(table, data, ignore_unknown_values=True)
         if errors == []:
              print("BQ WRITE:- --- SUCCESS ---")
              ts = time.time()
              destination_blob_name = "user/"+str(pubsub_message_id)+".json"
              
         else:
              print("BQ WRTIE:- --- FAILED ---")
              print(errors)
              destination_blob_name = "user/"+"failed/data_"+str(pubsub_message_id)+".json"
         
         client_s = storage.Client()
         bucket = client_s.get_bucket(bucket_name)
         blob = bucket.blob(destination_blob_name)
         blob.upload_from_string(pubsub_message)
         print("GCS WRITE:- --- SUCCESS ---")
    except Exception as error:
        print(error)