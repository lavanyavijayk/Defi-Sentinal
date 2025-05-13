# DeFi Sentinel - Comprehensive Fraud Detection System for Decentralized Exchanges

## Project Overview

DeFi Sentinel is an advanced cryptocurrency fraud detection system that employs distributed computing and sophisticated analytics to identify suspicious activities in decentralized exchanges (DEX). The project implements a multi-layered approach combining rule-based detection, machine learning, and graph-based network analysis to provide robust protection against evolving fraud techniques in the cryptocurrency space.

### Key Objectives Achieved

- Developed three complementary pipelines for comprehensive cryptocurrency fraud detection
- Successfully implemented a modular architecture enabling detection at multiple levels of sophistication
- Delivered an end-to-end system deployed on Google Cloud Platform to ensure scalability and real-time processing
- Created an integrated data warehouse for unified analysis and visualization of detected anomalies

## Pipeline Architecture

The system consists of three specialized, complementary detection pipelines:

### 1. Rule-Based Batch Processing

This pipeline applies established rules to identify known fraud patterns in historical blockchain data:

- **Flash Trade Detection**: Identifies rapid buying and selling of tokens to manipulate prices
- **Same Token Swap Analysis**: Detects circular trades that create artificial volume
- **Rapid Transaction Sequencing**: Spots unusual patterns of high-frequency trades that may indicate market manipulation
- **Implemented using**: Apache Spark on Google Dataproc for distributed batch processing of blockchain data
- **Trigger mechanism**: Cloud Functions automatically initiate processing on schedule or when new data arrives
- **Significance**: Provides baseline detection of established fraud patterns with minimal false positives

### 2. ML-Based Stream Processing

This pipeline applies machine learning for real-time fraud detection of anomalies:

- **Isolation Forest Algorithm**: Uses statistical anomaly detection to identify outlier transactions
- **Real-time Processing**: Analyzes transaction data as it occurs on the blockchain
- **Temporal Pattern Recognition**: Identifies unusual timing patterns that deviate from normal trading behavior
- **Implemented using**: Apache Beam on Google Dataflow with containerized processing
- **Significance**: Enables detection of previously unknown fraud patterns and adapts to evolving threats

### 3. Graph-Based Network Analysis

This pipeline focuses on relationship analysis between wallets and transactions:

- **PageRank Analysis**: Identifies influential addresses in the transaction network
- **Community Detection**: Discovers related trading groups that may be collaborating
- **KMeans Clustering**: Groups wallets based on behavioral features (e.g., in/out-degree, volume ratios); flags addresses that are far from cluster centroids as anomalous
- **Cluster Analysis**: Finds anomalous behavior patterns across the network
- **Implemented using**: GraphFrames on Apache Spark for graph analytics
- **Significance**: Reveals sophisticated fraud schemes that involve multiple coordinated accounts and complex transaction patterns

## Source Code Structure

This repository contains the following key components:

### 1. Rule-Based Batch Processing (`/Batch_processing/`)

- **`spark-job.py`**: Main PySpark application that implements rule-based fraud detection algorithms
- **`cloud_functions/`**: Code that runs inside the cloud function that trigger the dataproc

### 2. ML-Based Stream Processing (`/beam_custom_pipeline/`)

- **`rules.py`**: Apache Beam pipeline for real-time fraud detection using Isolation Forest
- **`Dockerfile`**: Dockerfile to create the docker container
- **`gcp_docker.sh`**: Script for deploying the docker container in gcr and create data flex template

### 3. Graph-Based Analytics (`/Network_analysis_batch_processing/`)

- **`network_analysis.py`**: PySpark application using GraphFrames for network analysis

### 4. Integration and Infrastructure

- **`terraform.tf`**: Infrastructure as Code for deploying the complete system

### 5. Database Schema

- **`schema.json`**: Schema for the bigquery streaming data table
- **`schema_batch_processing.json`**: Schema for the bigquery rule based batch processing data table

## System Architecture and Data Flow

1. **Data Ingestion**: Transaction data from Ethereum blockchain enters the system via:
   - Historical data dumps for batch processing
   - Real-time Pub/Sub streams for live analysis

2. **Processing**: Each pipeline processes data independently:
   - Rule-based: Scheduled Dataproc jobs analyze historical transaction batches
   - ML-based: Continuous Dataflow jobs process streaming data
   - Graph-based: Scheduled analysis of transaction networks

3. **Integration**: All detections are consolidated in BigQuery for:
   - Cross-pipeline correlation of findings
   - Aggregated reporting and visualization
   - Historical analysis of detection patterns

4. **Alert Generation**: High-confidence fraud detections generate alerts for further investigation

## Significance and Impact

DeFi Sentinel addresses a critical need in the cryptocurrency ecosystem:

- **Comprehensive Coverage**: Detects fraud at multiple levels, from simple patterns to complex network schemes
- **Adaptability**: Combines fixed rules with machine learning to handle both known and emerging fraud patterns
- **Scalability**: Built on cloud infrastructure to handle growing transaction volumes
- **Integration**: Provides a unified view across different detection methods
- **Real-time Capabilities**: Enables timely reaction to suspicious activities
- **Reduced False Positives**: Multi-pipeline approach improves overall accuracy by corroborating detections

## Getting Started

To run this project in your own GCP environment:

1. Clone this repository and navigate to the repo.
2. Install requirements
   ```bash
   pip install -r requirements.txt
   ```
3. Add terraform.tfvars file with variables project_id and region
4. Zip the cloud functions for the batch processing pipeline trigger
   ```bash
   zip cloud_functions Batch_processing/cloud_functions/main.py Batch_processing/cloud_functions/requirements.txt
   ```
5. Deploy the container in GCR and create data flex template by running the commands inside beam_custom_pipeline/gcp_docker.sh file.
6. Deploy the infrastructure using Terraform:
   ```bash
   terraform init
   terraform plan -var-file="terraform.tfvars"
   terraform apply -var-file="terraform.tfvars"
   ```
7. Monitor pipeline execution through the GCP Console
8. Run the local file for streaming data
   ```bash
   python live_stream_data.py
   ```
9. Run the local file for batch processing of data
   ```bash
   python Fetch.py
   ```
10. Run the local file for visualization
    ```bash
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

## License

This project is licensed under the MIT License - see the LICENSE file for details.