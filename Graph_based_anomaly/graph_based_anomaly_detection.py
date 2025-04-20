from pyspark.sql import SparkSession
from graphframes import GraphFrame
import pyspark.sql.functions as F
import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

spark = SparkSession.builder \
    .appName("Crypto Graph Anomaly Detection") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.0-s_2.12") \
    .getOrCreate()

# Load transaction data
transactions_df = spark.read \
    .format("bigquery") \
    .option("table", "your-project:crypto_analytics.transactions") \
    .load()

# Create the graph
vertices = transactions_df.select(F.col("sender").alias("id")).distinct() \
    .union(transactions_df.select(F.col("receiver").alias("id")).distinct())

edges = transactions_df.select(
    F.col("sender").alias("src"),
    F.col("receiver").alias("dst"),
    F.col("amount").alias("amount"),
    F.col("timestamp").alias("timestamp")
)

graph = GraphFrame(vertices, edges)

# Calculate graph metrics for each address
in_deg = graph.inDegrees
out_deg = graph.outDegrees
pr = graph.pageRank(resetProbability=0.15, maxIter=10).vertices.select("id", "pagerank")

# Calculate transaction volumes
sent_volume = transactions_df.groupBy("sender") \
    .agg(F.sum("amount").alias("sent_volume"), 
         F.count("*").alias("transactions_sent"))

received_volume = transactions_df.groupBy("receiver") \
    .agg(F.sum("amount").alias("received_volume"),
         F.count("*").alias("transactions_received"))

# Combine all metrics
address_metrics = vertices \
    .join(in_deg, vertices.id == in_deg.id, "left").drop(in_deg.id) \
    .join(out_deg, vertices.id == out_deg.id, "left").drop(out_deg.id) \
    .join(pr, vertices.id == pr.id, "left").drop(pr.id) \
    .join(sent_volume, vertices.id == sent_volume.sender, "left").drop(sent_volume.sender) \
    .join(received_volume, vertices.id == received_volume.receiver, "left").drop(received_volume.receiver)

# Fill NAs with 0
address_metrics = address_metrics.fillna(0)

# Create feature vector for clustering
assembler = VectorAssembler(
    inputCols=["inDegree", "outDegree", "pagerank", "sent_volume", "received_volume",
               "transactions_sent", "transactions_received"],
    outputCol="features"
)
address_features = assembler.transform(address_metrics)

# Normalize features
from pyspark.ml.feature import StandardScaler
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)
scaler_model = scaler.fit(address_features)
scaled_addresses = scaler_model.transform(address_features)

# Train KMeans to find clusters of addresses
kmeans = KMeans(k=20, seed=1, featuresCol="scaled_features")
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
    import numpy as np
    cluster_id = point[0]
    features = point[1]
    centroid = centroids[cluster_id]
    return float(np.sqrt(np.sum((np.array(features) - np.array(centroid)) ** 2)))

from pyspark.sql.types import DoubleType
spark.udf.register("distance_to_centroid", 
                   lambda point, centroids: distance_to_centroid_udf(point, centroids), 
                   DoubleType())

# Convert centroids to broadcast variable
centroids = model.clusterCenters()
centroids_broadcast = spark.sparkContext.broadcast(centroids)

# Calculate distance for each address
from pyspark.sql.functions import udf
distance_udf = udf(lambda cluster, features: distance_to_centroid_udf(
    (cluster, features), centroids_broadcast.value), DoubleType())

clustered_addresses = clustered_addresses.withColumn(
    "distance_to_centroid",
    distance_udf(F.col("prediction"), F.col("scaled_features"))
)

# Find outliers (addresses far from their cluster centroid)
outliers = clustered_addresses.orderBy(F.desc("distance_to_centroid")).limit(500)

# Combine anomalous addresses and outliers
anomalies = anomalous_addresses.union(outliers).distinct()

# Mark addresses with unusual in/out volume ratio
volume_ratio_anomalies = address_metrics.withColumn(
    "volume_ratio",
    F.when(F.col("received_volume") > 0, 
          F.col("sent_volume") / F.col("received_volume")).otherwise(0)
)

high_ratio_anomalies = volume_ratio_anomalies.filter(F.col("volume_ratio") > 10)
low_ratio_anomalies = volume_ratio_anomalies.filter((F.col("volume_ratio") < 0.1) & 
                                                    (F.col("received_volume") > 0))

# Save all anomalies
anomalies.write \
    .format("bigquery") \
    .option("table", "your-project:crypto_analytics.graph_based_anomalies") \
    .mode("overwrite") \
    .save()

high_ratio_anomalies.write \
    .format("bigquery") \
    .option("table", "your-project:crypto_analytics.volume_ratio_high_anomalies") \
    .mode("overwrite") \
    .save()

low_ratio_anomalies.write \
    .format("bigquery") \
    .option("table", "your-project:crypto_analytics.volume_ratio_low_anomalies") \
    .mode("overwrite") \
    .save()