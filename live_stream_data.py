import datetime
import requests
import json
from google.cloud import pubsub_v1

BITQUERY_API_URL = "https://graphql.bitquery.io"
BITQUERY_API_KEY = ""
ACCESS_TOKEN = ""
PROJECT_ID = ""
TOPIC_ID = "data-topic"


def fetch_dex_trades():
    now = datetime.datetime.now()
    ten_minutes_ago = now - datetime.timedelta(minutes=1)
    formatted_start_time = ten_minutes_ago.strftime("%Y-%m-%dT%H:%M:%S")
    # Query with time filter and timestamp in the response
    query = """
{
  ethereum(network: ethereum) {
    dexTrades(
      options: {limit: 20}
      time: {since: "%s"}
      protocol: {is: "Uniswap v2"}
    ) {
      protocol
      block {
        timestamp {
          time(format: "%%Y-%%m-%%d %%H:%%M:%%S")
          unixtime
        }
        height
      }
      transaction {
        hash
      }
      buyCurrency {
        symbol
        address
      }
      buyAmount
      sellCurrency {
        symbol
        address
      }
      sellAmount
      trades: count
      gasValue
      gasPrice
    }
  }
}
""" % formatted_start_time

    headers = {

        "Content-Type": "application/json",
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    response = requests.post(
        BITQUERY_API_URL,
        json={"query": query},
        headers=headers
    )

    if response.status_code == 200:
        return response.json()
    else:
        print("Query failed", response.status_code)
        print(response.text)
        return None

def publish_to_pubsub(data: dict):
    publisher = pubsub_v1.PublisherClient.from_service_account_file("defi-sentinal-e60499e85114.json")
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    message_json = json.dumps(data)
    message_bytes = message_json.encode("utf-8")
    print(message_json)
    future = publisher.publish(topic_path, data=message_bytes)
    print(f"Published message ID: {future.result()}")

import time
# Main execution
while True:
    result = fetch_dex_trades()
    if result and not result.get("errors", None):
        publish_to_pubsub(result)
        time.sleep(65)
                







#   #!/usr/bin/env python3
# """
# Simple script to upload a file to the procket-batch-bucket GCS bucket.
# """

# from google.cloud import storage
# import os
# import sys

# def upload_to_bucket(source_file_path):
#     """
#     Uploads a file to the procket-batch-bucket/data/ folder
    
#     Args:
#         source_file_path: Path to the local file to upload
#     """
#     # Set the bucket name
#     bucket_name = "procket-batch-bucket"
    
#     # Get the filename from the path
#     file_name = os.path.basename(source_file_path)
    
#     # Set destination path with data/ prefix
#     destination_blob_name = f"data/{file_name}"
    
#     try:
#         # Initialize the GCS client
#         storage_client = storage.Client()
        
#         # Get the bucket
#         bucket = storage_client.bucket(bucket_name)
        
#         # Create blob object
#         blob = bucket.blob(destination_blob_name)
        
#         # Upload the file
#         print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
#         blob.upload_from_filename(source_file_path)
        
#         print(f"File {file_name} uploaded successfully to gs://{bucket_name}/{destination_blob_name}")
#         return True
        
#     except Exception as e:
#         print(f"Error uploading file: {e}")
#         return False

# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: python upload_script.py <path_to_file>")
#         sys.exit(1)
    
#     file_path = sys.argv[1]
    
#     if not os.path.exists(file_path):
#         print(f"Error: File {file_path} does not exist")
#         sys.exit(1)
        
#     upload_to_bucket(file_path)






#   #!/usr/bin/env python3
# """
# Simple script to upload a file to the procket-batch-bucket GCS bucket.
# """

# from google.cloud import storage
# import os
# import sys

# def upload_to_bucket(source_file_path):
#     """
#     Uploads a file to the procket-batch-bucket/data/ folder
    
#     Args:
#         source_file_path: Path to the local file to upload
#     """
#     # Set the bucket name
#     bucket_name = "procket-batch-bucket"
    
#     # Get the filename from the path
#     file_name = os.path.basename(source_file_path)
    
#     # Set destination path with data/ prefix
#     destination_blob_name = f"data/{file_name}"
    
#     try:
#         # Initialize the GCS client
#         storage_client = storage.Client()
        
#         # Get the bucket
#         bucket = storage_client.bucket(bucket_name)
        
#         # Create blob object
#         blob = bucket.blob(destination_blob_name)
        
#         # Upload the file
#         print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
#         blob.upload_from_filename(source_file_path)
        
#         print(f"File {file_name} uploaded successfully to gs://{bucket_name}/{destination_blob_name}")
#         return True
        
#     except Exception as e:
#         print(f"Error uploading file: {e}")
#         return False

# if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: python upload_script.py <path_to_file>")
#         sys.exit(1)
    
#     file_path = sys.argv[1]
    
#     if not os.path.exists(file_path):
#         print(f"Error: File {file_path} does not exist")
#         sys.exit(1)
        
#     upload_to_bucket(file_path)