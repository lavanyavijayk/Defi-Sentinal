import datetime
import requests
import json
import os
import sys
from google.cloud import storage  # Added missing import
import time
BITQUERY_API_URL = "https://graphql.bitquery.io"
BITQUERY_API_KEY = ""
ACCESS_TOKEN = ""
PROJECT_ID = ""
TOPIC_ID = "data-topic"

def fetch_dex_trades(formatted_start_time):
    # Query with time filter and timestamp in the response
    query = """
    {
     ethereum(network: ethereum) {
       dexTrades(
         options: {limit: 10}
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

def save_to_file(data):
    """Save the fetched data to a local file with timestamp as a single-line JSON"""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"dex_trades_{timestamp}.json"
    
    with open(filename, 'w') as f:
        # Save as single-line JSON by setting indent=None and separators to compact representation
        json.dump(data, f, indent=None, separators=(',', ':'))
    
    print(f"Data saved to {filename}")
    return filename

def upload_to_bucket(source_file_path):
    """
    Uploads a file to the procket-batch-bucket/data/ folder
    Args:
    source_file_path: Path to the local file to upload
    """
    # Set the bucket name
    bucket_name = "defi-sentinal-batch-processing-bucket"
    # Get the filename from the path
    file_name = os.path.basename(source_file_path)
    # Set destination path with data/ prefix
    destination_blob_name = f"data/input/{file_name}"
    
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        # Get the bucket
        bucket = storage_client.bucket(bucket_name)
        # Create blob object
        blob = bucket.blob(destination_blob_name)
        # Upload the file
        print(f"Uploading {source_file_path} to gs://{bucket_name}/{destination_blob_name}...")
        blob.upload_from_filename(source_file_path)
        print(f"File {file_name} uploaded successfully to gs://{bucket_name}/{destination_blob_name}")
        return True
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False

def delete_local_file(file_path):
    """Delete the local file after successful upload"""
    try:
        os.remove(file_path)
        print(f"Local file {file_path} deleted successfully")
        return True
    except Exception as e:
        print(f"Error deleting local file: {e}")
        return False

def main():
    # Fetch the data
    now = datetime.datetime.now()
    ten_minutes_ago = now - datetime.timedelta(minutes=2)
    formatted_start_time = ten_minutes_ago.strftime("%Y-%m-%dT%H:%M:%S")
    result = fetch_dex_trades(formatted_start_time)
    
    if result and not result.get("errors", None):
        # Save to file with timestamp
        local_file_path = save_to_file(result)
        
        # Upload to GCS
        upload_success = upload_to_bucket(local_file_path)
        
        if upload_success:
            # Delete local file after successful upload
            delete_local_file(local_file_path)
            print("Process completed successfully!")
        else:
            print("Upload failed, but data was fetched and saved locally.")
    else:
        print("Failed to fetch data.")

if __name__ == "__main__":
    while True:
      main()
      time.sleep(122)