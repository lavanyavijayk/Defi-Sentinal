# Defi-Sentinal

## Project Overview

DeFi Sentinel is a comprehensive cryptocurrency fraud detection system that leverages distributed computing and advanced analytics to identify suspicious activities in decentralized exchanges (DEX). The project implements a multi-layered approach that combines rule-based detection, machine learning, and graph-based network analysis to provide robust protection against evolving fraud techniques in the cryptocurrency space.

### Objectives Achieved

- Successfully developed three complementary pipelines for cryptocurrency fraud detection
- Implemented a rule-based system for detecting known fraud patterns
- Created a real-time streaming pipeline with machine learning for statistical anomaly detection
- Built a graph analytics pipeline that identifies network-level suspicious activities
- Integrated results into a unified BigQuery data warehouse for comprehensive analysis
- Deployed the entire system on Google Cloud Platform using distributed computing services

## Source Code Structure

This repository contains the following key components:

### 1. Rule-Based Batch Processing (`/Batch_processing/`)

- **`spark-job.py`**: Main PySpark application that implements rule-based fraud detection algorithms including:
  - Flash trade detection
  - Same token swap identification
  - Rapid transaction sequencing
- **`cloud_functions/`**: Code that runs inside the cloud function that trigger the dataproc

### 2. ML-Based Stream Processing (`/beam_custom_pipeline/`)

- **`rules.py`**: Apache Beam pipeline for real-time fraud detection using Isolation Forest
- **`Dockerfile`**: Dockerfile to create the docker container
- **`gcp_docker.sh`**: Script for deploying the docker container in gcr and create data flex template

### 3. Graph-Based Analytics (`/Network_analysis_batch_processing/`)

- **`network_analysis.py`**: PySpark application using GraphFrames for network analysis including:
  - PageRank calculation for influential addresses
  - Community detection for identifying related trading groups
  - Clustering for finding anomalous behavior patterns

### 4. Integration and Infrastructure (`/infrastructure/`)

- **`terraform.tf`**: Infrastructure as Code for deploying the complete system

### 5. Database Schema

-**`schema.json`**: Schema for the bigquery streaming data table
-**`schema_batch_processing.json`**: Schema for the bigquery rule based batch processing data table

## Getting Started

To run this project in your own GCP environment:

1. Clone this repository and navigate to the repo.
2. Install requirements
    ```
    pip install -r requirements.txt
    ```
3. Add terraform.tfvars file with variables project_id and region
4. Zip the cloud functions for the batch processing pipeline trigger
    ```
    zip cloud_functions Batch_processing/cloud_functions/main.py Batch_processing/cloud_functions/requirements.txt
    ```
5. Deploy the container in gcr and create data flex template by running the commands inside beam_custom_pipeline/gcp_docker.sh file.
6. Deploy the infrastructure using Terraform:
   ```
   terraform init
   terraform plan -var-file="terraform.tfvars"
   terraform apply -var-file="terraform.tfvars"
   ```
7. Monitor pipeline execution through the GCP Console
8. Run the local file for streaming data
    ```
    python live_stream_data.py
    ```
9. Run the local file for batch processing of data
    ```
    python Fetch.py
    ```
10. Run the local file for visualization
    ```
    python visualization.py
    ```
## Requirements

- Google Cloud Platform account with billing enabled
- The following GCP services:
  - Cloud Storage
  - Pub/Sub
  - Dataflow
  - Dataproc
  - BigQuery
  - Cloud Functions
  - Cloud Scheduler
- Apache Spark 3.0+
- Python 3.8+
- Apache Beam 2.40+

