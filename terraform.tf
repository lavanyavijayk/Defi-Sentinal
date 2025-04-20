# terraform_pipeline_setup.tf

provider "google" {
  credentials = file("defi-sentinal-e60499e85114.json")
  project     = var.project_id
  region      = var.region
}

provider "google-beta" {
  credentials = file("defi-sentinal-e60499e85114.json")
  project = var.project_id
  region  = var.region
  alias   = "beta"
}

resource "google_project_service" "enable_services" {
  for_each = toset([
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "dataflow.googleapis.com",
    "dataproc.googleapis.com",
    "storage.googleapis.com",
    "cloudfunctions.googleapis.com"
  ])
  project                    = var.project_id
  service                    = each.key
  disable_dependent_services = true  # Allow disabling dependent services
}

# Create Pub/Sub Topic
resource "google_pubsub_topic" "data_topic" {
  name = "data-topic"
  lifecycle {
    prevent_destroy = false
  }
}

# Create Pub/Sub Subscription (Dataflow will subscribe to this)
resource "google_pubsub_subscription" "data_subscription" {
  name  = "data-subscription"
  topic = google_pubsub_topic.data_topic.name
  ack_deadline_seconds = 20
  depends_on = [google_pubsub_topic.data_topic]
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "streaming_dataset" {
  dataset_id = "streaming_dataset"
  location   = var.region
  lifecycle {
    prevent_destroy = false
  }
}

# Create BigQuery Table using an external schema file (schema.json)
resource "google_bigquery_table" "data_table" {
  dataset_id = google_bigquery_dataset.streaming_dataset.dataset_id
  table_id   = "trades_table"
  schema     = file("schema.json")
  deletion_protection = false 
  depends_on = [google_bigquery_dataset.streaming_dataset]
  lifecycle {
    prevent_destroy = false
  }
}

# Create a Cloud Storage Bucket to store the Dataflow pipeline file
resource "google_storage_bucket" "dataflow_bucket" {
  name          = "${var.project_id}-dataflow-bucket"
  location      = var.region
  force_destroy = true
  lifecycle {
    prevent_destroy = false
  }
}

#Provision the Dataflow Flex Template Job
resource "google_dataflow_flex_template_job" "dataflow_job" {
  provider                = google-beta.beta
  name                    = "dataflow-job"
  container_spec_gcs_path = "gs://defi-sentinal-dataflow-bucket/templates/dataflow_job_template.json"
  on_delete               = "cancel"
  parameters = {
    inputTopic  = "projects/${var.project_id}/topics/${google_pubsub_topic.data_topic.name}"
    outputTable = "${var.project_id}:${google_bigquery_dataset.streaming_dataset.dataset_id}.trades_table"
    maxNumWorkers = "3"
    enableStreamingEngine = "true"
    # Set up idempotence for PubSub source
    idempotentWriteToBigQuery = "true"
  }
  depends_on = [
    google_pubsub_subscription.data_subscription,
    google_bigquery_table.data_table
  ]
}

#=========================================================
# Instance provisioning for the batch data for network analysis(continuation of streaming data pipeline)

# Create the Dataproc bucket
resource "google_storage_bucket" "dataproc_bucket" {
  name          = "${var.project_id}-dataproc-bucket"
  location      = var.region
  force_destroy = true
  lifecycle {
    prevent_destroy = false
  }
}

# Create BigQuery Dataset for batch processed data
resource "google_bigquery_dataset" "network_analysis_dataset" {
  dataset_id = "network_analysis_dataset"
  location   = var.region
  lifecycle {
    prevent_destroy = false
  }
}

# Create BigQuery Dataset for batch processed data
resource "google_bigquery_dataset" "graph_based_anomaly" {
  dataset_id = "graph_based_anomaly"
  location   = var.region
  lifecycle {
    prevent_destroy = false
  }
}

resource "google_storage_bucket_object" "network_analysis_script" {
  name   = "templates/network_analysis.py"   # This sets the path inside the bucket
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "Network_analysis_batch_processing/network_analysis.py"            
  content_type = "text/x-python"
}

############################################################
# Network Analysis Dataset Tables
############################################################

# Important addresses identified by PageRank
resource "google_bigquery_table" "important_addresses" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "important_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "symbol",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token symbol"
    },
    {
      "name": "pagerank",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "PageRank score"
    },
    {
      "name": "total_transactions",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Total number of transactions"
    },
    {
      "name": "total_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total transaction volume"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

# Network communities (connected components)
resource "google_bigquery_table" "network_communities" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "network_communities"
  schema = jsonencode([
    {
      "name": "component",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Community identifier"
    },
    {
      "name": "count",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of addresses in the community"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

# Top addresses by in-degree (receiving transactions)
resource "google_bigquery_table" "in_degree_addresses" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "in_degree_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "symbol",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token symbol"
    },
    {
      "name": "inDegree",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of unique addresses that sent to this address"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

# Top addresses by out-degree (sending transactions)
resource "google_bigquery_table" "out_degree_addresses" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "out_degree_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "symbol",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token symbol"
    },
    {
      "name": "outDegree",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of unique addresses this address sent to"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

# Members of top communities
resource "google_bigquery_table" "community_members" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "community_members"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "component",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Community identifier"
    },
    {
      "name": "symbol",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token symbol"
    },
    {
      "name": "total_transactions",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Total number of transactions"
    },
    {
      "name": "total_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total transaction volume"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

# Token pair trading patterns
resource "google_bigquery_table" "token_pairs" {
  dataset_id = google_bigquery_dataset.network_analysis_dataset.dataset_id
  table_id   = "token_pairs"
  schema = jsonencode([
    {
      "name": "buy_token",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token being bought"
    },
    {
      "name": "sell_token",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Token being sold"
    },
    {
      "name": "count",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of trades between these tokens"
    },
    {
      "name": "analysis_timestamp",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the analysis was performed"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.network_analysis_dataset]
}

############################################################
# Graph-Based Anomaly Dataset Tables
############################################################

# Addresses flagged by cluster-based anomaly detection
resource "google_bigquery_table" "graph_based_anomalous_addresses" {
  dataset_id = google_bigquery_dataset.graph_based_anomaly.dataset_id
  table_id   = "graph_based_anomalous_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "prediction",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Cluster prediction"
    },
    {
      "name": "distance_to_centroid",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Distance to cluster centroid"
    },
    {
      "name": "inDegree",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "In-degree of the address"
    },
    {
      "name": "outDegree",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Out-degree of the address"
    },
    {
      "name": "pagerank",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "PageRank score"
    },
    {
      "name": "sent_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume sent"
    },
    {
      "name": "received_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume received"
    },
    {
      "name": "transactions_sent",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions sent"
    },
    {
      "name": "transactions_received",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions received"
    },
    {
      "name": "detection_time",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the anomaly was detected"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.graph_based_anomaly]
}

# Addresses with unusually high sent/received volume ratio
resource "google_bigquery_table" "volume_ratio_high_anomalous_addresses" {
  dataset_id = google_bigquery_dataset.graph_based_anomaly.dataset_id
  table_id   = "volume_ratio_high_anomalous_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "volume_ratio",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Ratio of sent volume to received volume"
    },
    {
      "name": "sent_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume sent"
    },
    {
      "name": "received_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume received"
    },
    {
      "name": "transactions_sent",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions sent"
    },
    {
      "name": "transactions_received",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions received"
    },
    {
      "name": "detection_time",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the anomaly was detected"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.graph_based_anomaly]
}

# Addresses with unusually low sent/received volume ratio
resource "google_bigquery_table" "volume_ratio_low_anomalous_addresses" {
  dataset_id = google_bigquery_dataset.graph_based_anomaly.dataset_id
  table_id   = "volume_ratio_low_anomalous_addresses"
  schema = jsonencode([
    {
      "name": "id",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Address identifier"
    },
    {
      "name": "volume_ratio",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Ratio of sent volume to received volume"
    },
    {
      "name": "sent_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume sent"
    },
    {
      "name": "received_volume",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Total volume received"
    },
    {
      "name": "transactions_sent",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions sent"
    },
    {
      "name": "transactions_received",
      "type": "INTEGER",
      "mode": "NULLABLE",
      "description": "Number of transactions received"
    },
    {
      "name": "detection_time",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the anomaly was detected"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.graph_based_anomaly]
}

# Anomalous transactions table
resource "google_bigquery_table" "anomalous_transactions" {
  dataset_id = google_bigquery_dataset.graph_based_anomaly.dataset_id
  table_id   = "anomalous_transactions"
  schema = jsonencode([
    {
      "name": "tx_hash",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Transaction hash"
    },
    {
      "name": "analyzed",
      "type": "BOOLEAN",
      "mode": "NULLABLE",
      "description": "Whether the transaction has been analyzed"
    },
    {
      "name": "anomaly_score",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Anomaly score"
    },
    {
      "name": "detection_time",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the anomaly was detected"
    },
    {
      "name": "reason",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Reason for flagging as anomalous"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.graph_based_anomaly]
}

# Processed transactions tracking table
resource "google_bigquery_table" "processed_transactions" {
  dataset_id = google_bigquery_dataset.graph_based_anomaly.dataset_id
  table_id   = "processed_transactions"
  schema = jsonencode([
    {
      "name": "tx_hash",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Transaction hash"
    },
    {
      "name": "analyzed",
      "type": "BOOLEAN",
      "mode": "NULLABLE",
      "description": "Whether the transaction has been analyzed"
    },
    {
      "name": "detection_time",
      "type": "TIMESTAMP",
      "mode": "NULLABLE",
      "description": "When the transaction was processed"
    },
    {
      "name": "anomaly_score",
      "type": "FLOAT",
      "mode": "NULLABLE",
      "description": "Anomaly score if anomalous, null otherwise"
    },
    {
      "name": "reason",
      "type": "STRING",
      "mode": "NULLABLE",
      "description": "Reason for flagging as anomalous, if applicable"
    }
  ])
  deletion_protection = false
  depends_on          = [google_bigquery_dataset.graph_based_anomaly]
}

# Create a scheduled job to run the analysis periodically
resource "google_dataproc_workflow_template" "dex_analysis_template" {
  name        = "dex-analysis-workflow"
  location    = var.region
  placement {
    managed_cluster {
      cluster_name = "dex-analysis-ephemeral"
      config {
        gce_cluster_config {
          zone = "${var.region}-a"
        }
        master_config {
          num_instances = 1
          machine_type  = "n1-standard-4"
          disk_config {
            boot_disk_type    = "pd-standard"
            boot_disk_size_gb = 350
          }
        }
        worker_config {
          num_instances = 2
          machine_type  = "n1-standard-4"
          disk_config {
            boot_disk_type    = "pd-standard"
            boot_disk_size_gb = 350
          }
        }
        software_config {
          image_version = "2.0-debian10"
          optional_components = ["JUPYTER"]
        }
      }
    }
  }

  jobs {
    step_id = "dex-network-analysis"
    pyspark_job {
      main_python_file_uri = "gs://${google_storage_bucket.dataproc_bucket.name}/templates/network_analysis.py"
      properties = {
        "spark.jars.packages" = "graphframes:graphframes:0.8.2-spark3.0-s_2.12"
      }
      jar_file_uris = ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    }
  }
}

resource "google_cloud_scheduler_job" "dex_analysis_scheduler" {
  name             = "dex-analysis-hourly-trigger"
  description      = "Triggers DEX network analysis workflow template every hour"
  schedule         = "0 * * * *"  # Every hour (fixed syntax)
  time_zone        = "UTC"
  region           = var.region
  
  http_target {
    http_method = "POST"
    uri         = "https://dataproc.googleapis.com/v1/projects/${var.project_id}/regions/${var.region}/workflowTemplates/${google_dataproc_workflow_template.dex_analysis_template.name}:instantiate"
    
    oauth_token {
      service_account_email = "defi-107@defi-sentinal.iam.gserviceaccount.com"
    }
  }
  depends_on = [google_dataproc_workflow_template.dex_analysis_template]
}

#=========================================================
# Instance provisioning for the batch data for rule based analysis

# Provision a GCS bucket for batch files
resource "google_storage_bucket" "batch_bucket" {
  name          = "${var.project_id}-batch-processing-bucket"
  location      = var.region
  force_destroy = true
}


# Upload the Spark Python script to the "templates/" folder in the bucket
resource "google_storage_bucket_object" "spark_python_script" {
  name   = "templates/spark-job.py"   # This sets the path inside the bucket
  bucket = google_storage_bucket.dataproc_bucket.name
  source = "Batch_processing/spark-job.py"             # Path to your local spark-job.py file
  content_type = "text/x-python"
}

# Create BigQuery Dataset for batch processed data
resource "google_bigquery_dataset" "batch_data_dataset" {
  dataset_id = "batch_data_dataset"
  location   = var.region
  lifecycle {
    prevent_destroy = false
  }
}

# Create BigQuery Table for batch processed data
resource "google_bigquery_table" "batch_data_table" {
  dataset_id = google_bigquery_dataset.batch_data_dataset.dataset_id
  table_id   = "batch_processed_data"  # Updated table name to match usage in Cloud Function and Dataproc job
  schema     = file("schema_batch_processing.json")
  deletion_protection = false 
  depends_on = [google_bigquery_dataset.batch_data_dataset]
  lifecycle {
    prevent_destroy = false
  }
}


# Create a Dataproc cluster for batch processing
resource "google_dataproc_cluster" "batch_cluster" {
  name   = "batch-processing-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      # Removed explicit instance_names to let Dataproc auto-generate them
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
      # Removed explicit instance_names
    }
  }
}

resource "google_storage_bucket_object" "function_archive" {
  name   = "cloud_functions.zip"
  bucket = google_storage_bucket.batch_bucket.name
  source = "cloud_functions.zip"
}



# Cloud Function to trigger Dataproc Job when a file is added to the GCS bucket
resource "google_cloudfunctions_function" "trigger_dataproc" {
  name                  = "trigger-dataproc-job"
  description           = "Triggers Dataproc Spark job upon new file arrival in GCS bucket"
  runtime               = "python39"
  entry_point           = "trigger_dataproc"
  available_memory_mb   = 256
  source_archive_bucket = google_storage_bucket.batch_bucket.name
  source_archive_object = google_storage_bucket_object.function_archive.name

  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.batch_bucket.name
  }

  environment_variables = {
    CLUSTER_NAME  = google_dataproc_cluster.batch_cluster.name
    REGION        = var.region
    PROJECT       = var.project_id
    BQ_TABLE      = "${var.project_id}:${google_bigquery_dataset.batch_data_dataset.dataset_id}.batch_processed_data"
    SPARK_JAR_URI = "gs://${google_storage_bucket.dataproc_bucket.name}/templates/spark-job.py"
    GOOGLE_FUNCTION_SOURCE = "cloud_functions/main.py"
  }
  
  depends_on = [google_storage_bucket_object.function_archive]
}

# Fixed Dataproc Job resource
resource "google_dataproc_job" "batch_job" {
  region = var.region
  
  # Added required placement block
  placement {
    cluster_name = google_dataproc_cluster.batch_cluster.name
  }
  
  # Fixed job configuration structure
  pyspark_config {
    main_python_file_uri = "gs://${google_storage_bucket.dataproc_bucket.name}/templates/spark-job.py"
    args = [
      "gs://${google_storage_bucket.batch_bucket.name}/data/input",
      "${var.project_id}.${google_bigquery_dataset.batch_data_dataset.dataset_id}.batch_processed_data"
    ]
  }

  labels = {
    "job_type" = "batch_processing"
  }

  depends_on = [
    google_dataproc_cluster.batch_cluster,
    google_storage_bucket_object.spark_python_script
  ]
}