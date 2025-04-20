import os
import base64
import json
from google.cloud import dataproc_v1
from google.cloud import storage
import functions_framework
import logging
@functions_framework.cloud_event
def trigger_dataproc(cloud_event):
    """
    Cloud Function to be triggered by a Cloud Storage event.
    This function starts a Dataproc job when a new file is added to the configured bucket.
    
    Args:
        cloud_event: The Cloud Event that triggered this function
    """
    # Extract bucket and filename from the cloud event
    try:
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]
        logging.info(data)
        
        # Log the triggering event
        print(f"Function triggered by upload of {file_name} to {bucket_name}")
        
        # Only process files in the data/input directory
        if not file_name.startswith("data/input/"):
            print(f"Ignoring file {file_name} - not in the data/input directory")
            return
        
        
        # Get environment variables
        project_id = os.environ.get("PROJECT")
        region = os.environ.get("REGION")
        cluster_name = os.environ.get("CLUSTER_NAME") 
        bq_table = os.environ.get("BQ_TABLE")
        spark_jar_uri = os.environ.get("SPARK_JAR_URI")
        
        # Set up input file path
        gcs_input_uri = f"gs://{bucket_name}/{file_name}"
        
        # Print environment variables
        print(f"PROJECT: {project_id}")
        print(f"REGION: {region}")
        print(f"CLUSTER_NAME: {cluster_name}")
        print(f"BQ_TABLE: {bq_table}")
        print(f"SPARK_JAR_URI: {spark_jar_uri}")
        print(f"GCS_INPUT_URI: {gcs_input_uri}")
        # Validate environment variables
        if not all([project_id, region, cluster_name, bq_table, spark_jar_uri]):
            raise ValueError("Missing required environment variables. Please check configuration.")
            
        # Create Dataproc client
        job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )
        
        # Configure PySpark job
        job = {
            "placement": {"cluster_name": cluster_name},
            "pyspark_job": {
                "main_python_file_uri": spark_jar_uri,
                "args": [
                    gcs_input_uri,  # Input file
                    bq_table        # Output BigQuery table
                ]
            },
            "labels": {"job_type": "batch_processing"}
        }
        
        # Submit the job
        operation = job_client.submit_job_as_operation(
            request={
                "project_id": project_id,
                "region": region,
                "job": job
            }
        )
        
        print(f"Submitted job to Dataproc cluster {cluster_name}")
        
        # Get the job details
        result = operation.result()
        job_id = result.reference.job_id
        
        print(f"Job {job_id} submitted successfully")
        return f"Job {job_id} submitted successfully"
        
    except Exception as e:
        print(f"Error triggering Dataproc job: {str(e)}")
        raise e