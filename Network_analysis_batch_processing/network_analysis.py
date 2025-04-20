from pyspark.sql import SparkSession
from graphframes import GraphFrame
import pyspark.sql.functions as F
import numpy as np
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import udf, lit, current_timestamp, coalesce
import datetime
import traceback
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

# Initialize Spark session with GraphFrames
spark = SparkSession.builder \
    .appName("DEX Trades Analysis and Anomaly Detection") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("temporaryGcsBucket", "defi-sentinal-batch-processing-bucket") \
    .getOrCreate()

# Create logger first, then use it
log4jLogger = spark.sparkContext._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger("DEXTradesAnalysis")
logger.info("Starting combined DEX trades network analysis and anomaly detection")

# Set checkpoint directory for GraphFrames algorithms after logger is initialized
checkpoint_dir = "gs://defi-sentinal-dataproc-bucket/spark_checkpoints"
spark.sparkContext.setCheckpointDir(checkpoint_dir)
logger.info(f"Set checkpoint directory to: {checkpoint_dir}")

def load_transactions(analyzed_filter=None):
    """Load transactions from BigQuery with optional filter for analyzed status"""
    logger.info("Loading transactions from BigQuery")
    
    if analyzed_filter is not None:
        df = spark.read \
            .format("bigquery") \
            .option("table", "defi-sentinal.streaming_dataset.trades_table") \
            .option("filter", f"analyzed = {str(analyzed_filter).lower()}") \
            .load()
        logger.info(f"Loaded transactions with analyzed={analyzed_filter}")
    else:
        df = spark.read \
            .format("bigquery") \
            .option("table", "defi-sentinal.streaming_dataset.trades_table") \
            .load()
        logger.info("Loaded all transactions")
    
    return df

def create_graph(df):
    """Create a graph from transaction data"""
    logger.info("Creating graph from transaction data")
    
    # Create vertices DataFrame (unique addresses)
    vertices = df.select(F.col("buy_address").alias("id")).distinct() \
        .union(df.select(F.col("sell_address").alias("id")).distinct()) \
        .filter(F.col("id").isNotNull())
    
    # Add metadata to vertices by calculating total volume for each address
    # Fix: Use different aliases for the symbol columns to avoid ambiguity
    buy_volume = df.groupBy("buy_address") \
        .agg(
            F.sum("buy_amount").alias("total_buy_volume"),
            F.count("*").alias("buy_transactions"),
            F.first("buy_currency").alias("buy_symbol")  # Changed alias
        )
    
    sell_volume = df.groupBy("sell_address") \
        .agg(
            F.sum("sell_amount").alias("total_sell_volume"),
            F.count("*").alias("sell_transactions"),
            F.first("sell_currency").alias("sell_symbol")  # Changed alias
        )
    
    # Combine buy and sell stats for each address
    vertices = vertices \
        .join(buy_volume, vertices.id == buy_volume.buy_address, "left") \
        .drop("buy_address") \
        .join(sell_volume, vertices.id == sell_volume.sell_address, "left") \
        .drop("sell_address")
    
    # Create a single symbol column with coalesce to prioritize buy_symbol if available
    vertices = vertices.withColumn("symbol", coalesce("buy_symbol", "sell_symbol"))
    
    # Drop the individual symbol columns to avoid ambiguity
    vertices = vertices.drop("buy_symbol", "sell_symbol")
    
    # Fill null values with zeros
    vertices = vertices \
        .na.fill(0, ["total_buy_volume", "buy_transactions", "total_sell_volume", "sell_transactions"]) \
        .withColumn("total_transactions", F.col("buy_transactions") + F.col("sell_transactions")) \
        .withColumn("total_volume", F.col("total_buy_volume") + F.col("total_sell_volume"))
    
    # Create edges DataFrame - transactions between addresses
    edges = df.select(
        F.col("sell_address").alias("src"),
        F.col("buy_address").alias("dst"),
        F.col("buy_amount").alias("buy_value"),
        F.col("sell_amount").alias("sell_value"),
        F.col("tx_hash").alias("tx_hash"),
        F.col("gas_price").alias("gas_price"),
        F.col("block_unixtime").alias("timestamp")
    ) \
    .filter(F.col("src").isNotNull() & F.col("dst").isNotNull())
    
    # Aggregate edges for the same src-dst pair
    aggregated_edges = edges.groupBy("src", "dst") \
        .agg(
            F.sum("buy_value").alias("total_buy_value"),
            F.sum("sell_value").alias("total_sell_value"),
            F.count("*").alias("transaction_count"),
            F.avg("gas_price").alias("avg_gas_price"),
            F.max("timestamp").alias("latest_timestamp")
        ) \
        .withColumn("weight", F.col("transaction_count"))
    
    # Create the graph
    graph = GraphFrame(vertices, aggregated_edges)
    
    logger.info(f"Created graph with {vertices.count()} vertices and {aggregated_edges.count()} edges")
    
    return graph, vertices, edges, aggregated_edges

def run_network_analysis(graph, vertices, aggregated_edges, transactions_df):
    """Run network analysis on the graph"""
    logger.info("Running network analysis")
    
    # Run PageRank to find important addresses
    # Fix: The symbol column is now unambiguous since we resolved it in create_graph
    pagerank_results = graph.pageRank(resetProbability=0.15, maxIter=10)
    important_addresses = pagerank_results.vertices \
        .select("id", "symbol", "pagerank", "total_transactions", "total_volume") \
        .orderBy(F.desc("pagerank")) \
        .limit(100)
    
    # Find in-degree and out-degree
    in_degree = graph.inDegrees
    in_degree_ranked = in_degree.join(vertices.select("id", "symbol"), "id") \
        .orderBy(F.desc("inDegree")) \
        .limit(100)
    
    out_degree = graph.outDegrees
    out_degree_ranked = out_degree.join(vertices.select("id", "symbol"), "id") \
        .orderBy(F.desc("outDegree")) \
        .limit(100)
    
    # Find communities
    components = graph.connectedComponents()
    community_sizes = components.groupBy("component").count().orderBy(F.desc("count"))
    
    # Identify the top 10 communities
    top_communities = community_sizes.limit(10)
    top_community_ids = [row["component"] for row in top_communities.collect()]
    
    # For each top community, find the member addresses
    community_members = components \
    .filter(F.col("component").isin(top_community_ids)) \
    .join(
        vertices.select(
            F.col("id"),
            F.col("symbol").alias("token_symbol"),  # rename to avoid duplication
            F.col("total_transactions").alias("tx_count"),  # rename to avoid duplication
            F.col("total_volume").alias("volume")   # rename to avoid duplication
        ), 
        "id"
    )
    
    # Find token transaction patterns
    # Fix: Use the passed transactions_df parameter instead of assuming it's in scope
    token_pairs = transactions_df.select(
        F.col("buy_currency").alias("buy_token"),
        F.col("sell_currency").alias("sell_token")
    ) \
    .groupBy("buy_token", "sell_token") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(100)
    
    logger.info("Network analysis complete")
    
    return {
        "important_addresses": important_addresses,
        "in_degree_ranked": in_degree_ranked,
        "out_degree_ranked": out_degree_ranked,
        "community_sizes": community_sizes,
        "community_members": community_members,
        "token_pairs": token_pairs
    }

def run_anomaly_detection(graph, vertices, edges, transactions_df):
    """Run anomaly detection on the graph"""
    logger.info("Running anomaly detection")
    
    # Calculate graph metrics for each address
    in_deg = graph.inDegrees
    out_deg = graph.outDegrees
    pr = graph.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "pagerank")
    
    # Calculate transaction volumes
    sent_volume = transactions_df.groupBy("sell_address") \
        .agg(F.sum("sell_amount").alias("sent_volume"), 
             F.count("*").alias("transactions_sent"))
    
    received_volume = transactions_df.groupBy("buy_address") \
        .agg(F.sum("buy_amount").alias("received_volume"),
             F.count("*").alias("transactions_received"))
    
    # Combine all metrics
    address_metrics = vertices \
        .join(in_deg, vertices.id == in_deg.id, "left").drop(in_deg.id) \
        .join(out_deg, vertices.id == out_deg.id, "left").drop(out_deg.id) \
        .join(pr, vertices.id == pr.id, "left").drop(pr.id) \
        .join(sent_volume, vertices.id == sent_volume.sell_address, "left").drop(sent_volume.sell_address) \
        .join(received_volume, vertices.id == received_volume.buy_address, "left").drop(received_volume.buy_address)
    
    # Fill NAs with 0
    address_metrics = address_metrics.fillna(0)
    
    # Check if we have enough data for clustering
    if address_metrics.count() < 20:
        logger.warn("Not enough data for clustering-based anomaly detection")
        return {
            "volume_ratio_high": spark.createDataFrame([], "id STRING"),
            "volume_ratio_low": spark.createDataFrame([], "id STRING"),
            "cluster_anomalies": spark.createDataFrame([], "id STRING")
        }
    
    # Create feature vector for clustering
    assembler = VectorAssembler(
        inputCols=["inDegree", "outDegree", "pagerank", "sent_volume", "received_volume",
                   "transactions_sent", "transactions_received"],
        outputCol="features"
    )
    address_features = assembler.transform(address_metrics)
    
    # Normalize features
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    scaler_model = scaler.fit(address_features)
    scaled_addresses = scaler_model.transform(address_features)
    
    # Train KMeans to find clusters of addresses
    k = min(20, int(address_metrics.count() / 5))  # Adaptive k based on data size
    kmeans = KMeans(k=k, seed=1, featuresCol="scaled_features")
    model = kmeans.fit(scaled_addresses)
    clustered_addresses = model.transform(scaled_addresses)
    
    # Identify cluster sizes
    cluster_sizes = clustered_addresses.groupBy("prediction").count().orderBy("count")
    
    # Find anomalous addresses (those in small clusters)
    small_clusters = cluster_sizes.filter(F.col("count") < 10).select("prediction")
    small_cluster_list = [row["prediction"] for row in small_clusters.collect()]
    
    anomalous_addresses = clustered_addresses.filter(F.col("prediction").isin(small_cluster_list))
    
    # Calculate distance to centroid for all addresses
    def distance_to_centroid_udf(point, centroids):
        cluster_id = point[0]
        features = point[1]
        centroid = centroids[cluster_id]
        return float(np.sqrt(np.sum((np.array(features) - np.array(centroid)) ** 2)))
    
    # Register UDF
    spark.udf.register("distance_to_centroid", 
                       lambda point, centroids: distance_to_centroid_udf(point, centroids), 
                       DoubleType())
    
    # Convert centroids to broadcast variable
    centroids = model.clusterCenters()
    centroids_broadcast = spark.sparkContext.broadcast(centroids)
    
    # Calculate distance for each address
    distance_udf_func = udf(lambda cluster, features: distance_to_centroid_udf(
        (cluster, features), centroids_broadcast.value), DoubleType())
    
    # Add distance column to all clustered addresses
    clustered_addresses_with_distance = clustered_addresses.withColumn(
        "distance_to_centroid",
        distance_udf_func(F.col("prediction"), F.col("scaled_features"))
    )
    
    # Find outliers (addresses far from their cluster centroid)
    outliers = clustered_addresses_with_distance.orderBy(F.desc("distance_to_centroid")).limit(500)
    
    # Also add distance column to anomalous addresses (needed for union compatibility)
    anomalous_addresses_with_distance = anomalous_addresses.withColumn(
        "distance_to_centroid",
        distance_udf_func(F.col("prediction"), F.col("scaled_features"))
    )
    
    # Now both DataFrames have the same schema and can be union'd
    cluster_anomalies = anomalous_addresses_with_distance.union(outliers).distinct()
    
    # Mark addresses with unusual in/out volume ratio
    volume_ratio_anomalies = address_metrics.withColumn(
        "volume_ratio",
        F.when(F.col("received_volume") > 0, 
              F.col("sent_volume") / F.col("received_volume")).otherwise(0)
    )
    
    high_ratio_anomalies = volume_ratio_anomalies.filter(
        (F.col("volume_ratio") > 10) & 
        (F.col("sent_volume") > 0) &
        (F.col("transactions_sent") > 5)
    )
    
    low_ratio_anomalies = volume_ratio_anomalies.filter(
        (F.col("volume_ratio") < 0.1) & 
        (F.col("received_volume") > 0) &
        (F.col("transactions_received") > 5)
    )
    
    logger.info(f"Found {cluster_anomalies.count()} cluster-based anomalies, " +
                f"{high_ratio_anomalies.count()} high-ratio anomalies, " +
                f"{low_ratio_anomalies.count()} low-ratio anomalies")
    
    return {
        "volume_ratio_high": high_ratio_anomalies,
        "volume_ratio_low": low_ratio_anomalies,
        "cluster_anomalies": cluster_anomalies
    }

def setup_bigquery_tables_and_save_graph(graph, tmp_bucket):
    """
    Create necessary BigQuery tables and save graph structure for visualization
    
    Args:
        graph: GraphFrame object containing vertices and edges
        tmp_bucket: GCS bucket for temporary storage during BigQuery load
    """
    from google.cloud import bigquery
    import pyspark.sql.functions as F
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
    
    logger.info("Setting up BigQuery tables and saving graph structure")
    
    # Initialize BigQuery client
    bq_client = bigquery.Client(project="defi-sentinal")
    
    # Create the dataset if it doesn't exist
    dataset_id = "visualization_dataset"
    dataset_ref = bq_client.dataset(dataset_id)
    
    try:
        bq_client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        # Dataset does not exist, create it
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Specify the location
        dataset = bq_client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id}")
    
    # Define schema for vertices table
    vertices_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED", description="Unique address identifier"),
        bigquery.SchemaField("total_buy_volume", "FLOAT", mode="NULLABLE", description="Total buy volume"),
        bigquery.SchemaField("buy_transactions", "INTEGER", mode="NULLABLE", description="Number of buy transactions"),
        bigquery.SchemaField("total_sell_volume", "FLOAT", mode="NULLABLE", description="Total sell volume"),
        bigquery.SchemaField("sell_transactions", "INTEGER", mode="NULLABLE", description="Number of sell transactions"),
        bigquery.SchemaField("symbol", "STRING", mode="NULLABLE", description="Token symbol"),
        bigquery.SchemaField("total_transactions", "INTEGER", mode="NULLABLE", description="Total number of transactions"),
        bigquery.SchemaField("total_volume", "FLOAT", mode="NULLABLE", description="Total volume")
    ]
    
    # Define schema for edges table
    edges_schema = [
        bigquery.SchemaField("src", "STRING", mode="REQUIRED", description="Source address"),
        bigquery.SchemaField("dst", "STRING", mode="REQUIRED", description="Destination address"),
        bigquery.SchemaField("total_buy_value", "FLOAT", mode="NULLABLE", description="Total buy value"),
        bigquery.SchemaField("total_sell_value", "FLOAT", mode="NULLABLE", description="Total sell value"),
        bigquery.SchemaField("transaction_count", "INTEGER", mode="NULLABLE", description="Number of transactions"),
        bigquery.SchemaField("avg_gas_price", "FLOAT", mode="NULLABLE", description="Average gas price"),
        bigquery.SchemaField("latest_timestamp", "TIMESTAMP", mode="NULLABLE", description="Latest transaction timestamp"),
        bigquery.SchemaField("weight", "FLOAT", mode="NULLABLE", description="Edge weight")
    ]
    
    # Create vertices table if it doesn't exist
    vertices_table_id = "graph_vertices"
    vertices_table_ref = bq_client.dataset(dataset_id).table(vertices_table_id)
    
    try:
        bq_client.get_table(vertices_table_ref)
        logger.info(f"Table {dataset_id}.{vertices_table_id} already exists")
    except Exception:
        # Table does not exist, create it
        vertices_table = bigquery.Table(vertices_table_ref, schema=vertices_schema)
        vertices_table = bq_client.create_table(vertices_table)
        logger.info(f"Created table {dataset_id}.{vertices_table_id}")
    
    # Create edges table if it doesn't exist
    edges_table_id = "graph_edges"
    edges_table_ref = bq_client.dataset(dataset_id).table(edges_table_id)
    
    try:
        bq_client.get_table(edges_table_ref)
        logger.info(f"Table {dataset_id}.{edges_table_id} already exists")
    except Exception:
        # Table does not exist, create it
        edges_table = bigquery.Table(edges_table_ref, schema=edges_schema)
        edges_table = bq_client.create_table(edges_table)
        logger.info(f"Created table {dataset_id}.{edges_table_id}")
    
    # Fix data types in vertices DataFrame to match BigQuery schema
    vertices_with_types = graph.vertices \
        .withColumn("id", F.col("id").cast(StringType())) \
        .withColumn("total_buy_volume", F.col("total_buy_volume").cast(DoubleType())) \
        .withColumn("buy_transactions", F.col("buy_transactions").cast(IntegerType())) \
        .withColumn("total_sell_volume", F.col("total_sell_volume").cast(DoubleType())) \
        .withColumn("sell_transactions", F.col("sell_transactions").cast(IntegerType())) \
        .withColumn("symbol", F.col("symbol").cast(StringType())) \
        .withColumn("total_transactions", F.col("total_transactions").cast(IntegerType())) \
        .withColumn("total_volume", F.col("total_volume").cast(DoubleType()))
    
    # Fix data types in edges DataFrame to match BigQuery schema
    edges_with_types = graph.edges \
        .withColumn("src", F.col("src").cast(StringType())) \
        .withColumn("dst", F.col("dst").cast(StringType())) \
        .withColumn("total_buy_value", F.col("total_buy_value").cast(DoubleType())) \
        .withColumn("total_sell_value", F.col("total_sell_value").cast(DoubleType())) \
        .withColumn("transaction_count", F.col("transaction_count").cast(IntegerType())) \
        .withColumn("avg_gas_price", F.col("avg_gas_price").cast(DoubleType())) \
        .withColumn("latest_timestamp", F.col("latest_timestamp").cast(TimestampType())) \
        .withColumn("weight", F.col("weight").cast(DoubleType()))  # THIS IS THE KEY FIX
    
    logger.info("Data types adjusted to match BigQuery schema")
    
    # Print schema of edges to debug
    logger.info("Edge DataFrame schema after type conversion:")
    edges_with_types.printSchema()
    
    # Save vertices with all their attributes for visualization
    vertices_with_types.write \
        .format("bigquery") \
        .option("table", f"defi-sentinal.{dataset_id}.{vertices_table_id}") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Save edges with all their attributes for visualization
    edges_with_types.write \
        .format("bigquery") \
        .option("table", f"defi-sentinal.{dataset_id}.{edges_table_id}") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    logger.info("Graph structure saved to BigQuery successfully")
    
    # Return the created table references for use in visualization
    return {
        "vertices_table": f"defi-sentinal.{dataset_id}.{vertices_table_id}",
        "edges_table": f"defi-sentinal.{dataset_id}.{edges_table_id}"
    }

def save_results(network_results, anomaly_results, transactions_df):
    """Save analysis results to BigQuery"""
    from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
    logger.info("Saving results to BigQuery")
    
    # Use the bucket name only, without the gs:// prefix or any paths
    tmp_bucket = "defi-sentinal-batch-processing-bucket"
    
    # Add timestamp column to important_addresses
    network_results["important_addresses"] = network_results["important_addresses"].withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    # Save network analysis results
    network_results["important_addresses"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.important_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to in_degree_ranked
    network_results["in_degree_ranked"] = network_results["in_degree_ranked"].withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    network_results["in_degree_ranked"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.in_degree_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to out_degree_ranked
    network_results["out_degree_ranked"] = network_results["out_degree_ranked"].withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    network_results["out_degree_ranked"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.out_degree_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to community_sizes
    network_results["community_sizes"] = network_results["community_sizes"].withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    network_results["community_sizes"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.network_communities") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    logger.info("Community members column names:")
    logger.info(network_results["community_members"].columns)
    
    # In the save_results function, before writing to BigQuery:
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType

    # Get only the expected columns with no duplicates and add timestamp
    network_results["community_members"] = network_results["community_members"].select(
        F.col("id").cast(StringType()),
        F.col("component").cast(IntegerType()),
        # Take the first occurrence of each duplicate column
        F.col("token_symbol").alias("symbol").cast(StringType()),  # Updated to use token_symbol from renamed column
        F.col("tx_count").alias("total_transactions").cast(IntegerType()),  # Updated to use tx_count from renamed column
        F.col("volume").alias("total_volume").cast(DoubleType()),  # Updated to use volume from renamed column
        current_timestamp().alias("analysis_timestamp").cast(TimestampType())
    )

    # Handle any nulls to prevent issues
    network_results["community_members"] = network_results["community_members"] \
        .na.fill("", ["symbol"]) \
        .na.fill(0, ["component", "total_transactions", "total_volume"])

    # Log the final schema before writing
    logger.info("Final community members DataFrame schema before writing:")
    network_results["community_members"].printSchema()

    network_results["community_members"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.community_members") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to token_pairs
    network_results["token_pairs"] = network_results["token_pairs"].withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    network_results["token_pairs"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.network_analysis_dataset.token_pairs") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to cluster_anomalies (if not already present)
    anomaly_results["cluster_anomalies"] = anomaly_results["cluster_anomalies"].withColumn(
        "detection_time", current_timestamp().cast(TimestampType())
    )
    
    # Save anomaly detection results
    anomaly_results["cluster_anomalies"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.graph_based_anomaly.graph_based_anomalous_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to volume_ratio_high
    anomaly_results["volume_ratio_high"] = anomaly_results["volume_ratio_high"].withColumn(
        "detection_time", current_timestamp().cast(TimestampType())
    )
    
    anomaly_results["volume_ratio_high"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.graph_based_anomaly.volume_ratio_high_anomalous_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add timestamp column to volume_ratio_low
    anomaly_results["volume_ratio_low"] = anomaly_results["volume_ratio_low"].withColumn(
        "detection_time", current_timestamp().cast(TimestampType())
    )
    
    anomaly_results["volume_ratio_low"].write \
        .format("bigquery") \
        .option("table", "defi-sentinal.graph_based_anomaly.volume_ratio_low_anomalous_addresses") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Get transaction IDs associated with anomalous addresses
    cluster_anomaly_txs = transactions_df.filter(
        (F.col("buy_address").isin([row["id"] for row in anomaly_results["cluster_anomalies"].select("id").collect()])) |
        (F.col("sell_address").isin([row["id"] for row in anomaly_results["cluster_anomalies"].select("id").collect()]))
    ).select("tx_hash").distinct()
    
    high_ratio_txs = transactions_df.filter(
        (F.col("buy_address").isin([row["id"] for row in anomaly_results["volume_ratio_high"].select("id").collect()])) |
        (F.col("sell_address").isin([row["id"] for row in anomaly_results["volume_ratio_high"].select("id").collect()]))
    ).select("tx_hash").distinct()
    
    low_ratio_txs = transactions_df.filter(
        (F.col("buy_address").isin([row["id"] for row in anomaly_results["volume_ratio_low"].select("id").collect()])) |
        (F.col("sell_address").isin([row["id"] for row in anomaly_results["volume_ratio_low"].select("id").collect()]))
    ).select("tx_hash").distinct()
    
    # Prepare transaction updates
    cluster_tx_updates = cluster_anomaly_txs.withColumn("analyzed", lit(True)) \
        .withColumn("anomaly_score", lit(0.9)) \
        .withColumn("detection_time", current_timestamp()) \
        .withColumn("reason", lit("Graph-based anomaly detection"))
    
    high_ratio_tx_updates = high_ratio_txs.withColumn("analyzed", lit(True)) \
        .withColumn("anomaly_score", lit(0.85)) \
        .withColumn("detection_time", current_timestamp()) \
        .withColumn("reason", lit("Unusual high sent-to-received volume ratio"))
    
    low_ratio_tx_updates = low_ratio_txs.withColumn("analyzed", lit(True)) \
        .withColumn("anomaly_score", lit(0.82)) \
        .withColumn("detection_time", current_timestamp()) \
        .withColumn("reason", lit("Unusual low sent-to-received volume ratio"))
    
    # Combine all transaction updates
    all_anomaly_updates = cluster_tx_updates.union(high_ratio_tx_updates).union(low_ratio_tx_updates).distinct()
    
    # Create a DataFrame for all processed transactions
    all_processed = transactions_df.select("tx_hash").distinct() \
        .withColumn("analyzed", lit(True)) \
        .withColumn("detection_time", current_timestamp())
    
    # Add anomaly scores and reasons to the processed transactions
    all_processed_with_scores = all_processed.join(
        all_anomaly_updates.select("tx_hash", "anomaly_score", "reason"), 
        "tx_hash", 
        "left"
    )
    
    # Add analysis_timestamp to processed transactions
    all_processed_with_scores = all_processed_with_scores.withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    # Write transaction updates back to BigQuery
    all_processed_with_scores.write \
        .format("bigquery") \
        .option("table", "defi-sentinal.graph_based_anomaly.processed_transactions") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    # Add analysis_timestamp to anomalous transactions
    all_anomaly_updates = all_anomaly_updates.withColumn(
        "analysis_timestamp", current_timestamp().cast(TimestampType())
    )
    
    # Save only the anomalous transactions
    all_anomaly_updates.write \
        .format("bigquery") \
        .option("table", "defi-sentinal.graph_based_anomaly.anomalous_transactions") \
        .option("temporaryGcsBucket", tmp_bucket) \
        .mode("overwrite") \
        .save()
    
    logger.info("All results saved to BigQuery")

def mark_as_analyzed(transactions_df):
    """
    Updates the 'analyzed' field to True for all transactions that were processed
    
    Args:
        transactions_df: The original DataFrame of transactions that were analyzed
        bq_project: The BigQuery project ID
    """
    logger.info("Updating 'analyzed' status for processed transactions")
    bq_project="defi-sentinal"
    try:
        # Get all transaction hashes as a list
        tx_hashes_rows = transactions_df.select("tx_hash").distinct().collect()
        tx_hashes = [row["tx_hash"] for row in tx_hashes_rows]
        
        if not tx_hashes:
            logger.info("No transactions to update")
            return 0
            
        logger.info(f"Found {len(tx_hashes)} unique transaction hashes to mark as analyzed")
        
        # Initialize BigQuery client
        from google.cloud import bigquery
        bq_client = bigquery.Client(project=bq_project)
        
        # Build the UPDATE query with an IN clause
        # Note: For large sets of transactions, we'll need to batch this
        batch_size = 1000  # BigQuery has limits on query size
        rows_updated_total = 0
        
        for i in range(0, len(tx_hashes), batch_size):
            batch = tx_hashes[i:i+batch_size]
            
            # Format the transaction hashes for the SQL IN clause
            formatted_hashes = ", ".join([f"'{h}'" for h in batch])
            
            update_query = f"""
            UPDATE `{bq_project}.streaming_dataset.trades_table` 
            SET 
                analyzed = TRUE
            WHERE tx_hash IN ({formatted_hashes})
            """
            
            # Execute the update query
            logger.info(f"Executing update query for batch {i//batch_size + 1} with {len(batch)} transactions")
            query_job = bq_client.query(update_query)
            query_job.result()  # Wait for the query to complete
            
            rows_updated = query_job.num_dml_affected_rows
            rows_updated_total += rows_updated
            logger.info(f"Updated {rows_updated} transactions in batch {i//batch_size + 1}")
        
        logger.info(f"Successfully updated 'analyzed' field for {rows_updated_total} transactions in total")
        return rows_updated_total
        
    except Exception as e:
        logger.error(f"Error directly updating 'analyzed' status: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return 0
# Main execution flow
if __name__ == "__main__":
    try:
        # Load unanalyzed transactions
        transactions_df = load_transactions(analyzed_filter=False)
        
        # Skip processing if no unanalyzed transactions
        if transactions_df.count() == 0:
            logger.info("No unanalyzed transactions found. Exiting.")
            spark.stop()
            exit(0)
        
        # Create the graph
        graph, vertices, edges, aggregated_edges = create_graph(transactions_df)
        tmp_bucket = "defi-sentinal-batch-processing-bucket"
        graph_tables = setup_bigquery_tables_and_save_graph(graph, tmp_bucket)
        
        # Run network analysis
        # Fix: Pass transactions_df as a parameter to run_network_analysis
        network_results = run_network_analysis(graph, vertices, aggregated_edges, transactions_df)
        
        # Run anomaly detection
        anomaly_results = run_anomaly_detection(graph, vertices, edges, transactions_df)
        
        # Save all results
        save_results(network_results, anomaly_results, transactions_df)
        
        # Note about updating the main table
        mark_as_analyzed(transactions_df)
        
        # Print summary statistics
        print(f"Total vertices (unique addresses): {vertices.count()}")
        print(f"Total edges (transaction relationships): {aggregated_edges.count()}")
        print(f"Total communities identified: {network_results['community_sizes'].count()}")
        print(f"Largest community size: {network_results['community_sizes'].first()['count'] if network_results['community_sizes'].count() > 0 else 0}")
        print(f"Most important address (by PageRank): {network_results['important_addresses'].first()['id'] if network_results['important_addresses'].count() > 0 else 'None'}")
        print(f"Total anomalies detected: {anomaly_results['cluster_anomalies'].count() + anomaly_results['volume_ratio_high'].count() + anomaly_results['volume_ratio_low'].count()}")
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        # Fix: Remove exc_info parameter which was causing the error
        logger.error(f"Error in analysis: {str(e)}")
        # Alternative approach to log the stack trace
        logger.error(f"Stack trace: {traceback.format_exc()}")
        raise
    finally:
        spark.stop()