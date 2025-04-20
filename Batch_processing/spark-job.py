from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import udf, col, lit, explode, struct, count, window, when, unix_timestamp, from_unixtime, to_timestamp
from pyspark.sql.types import StringType, ArrayType, BooleanType, MapType
import sys
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define fraud detection rules
def evaluate_fraud(buyAmount, sellAmount, buySymbol, sellSymbol, transactionHash):
    result = {}
    
    # Rule 1: Flash Trade Volume
    FLASH_TRADE_THRESHOLD = 1000000  # Threshold set at 1,000,000 tokens
    is_flash_trade = False
    if buyAmount is not None and buyAmount > FLASH_TRADE_THRESHOLD:
        is_flash_trade = True
    if sellAmount is not None and sellAmount > FLASH_TRADE_THRESHOLD:
        is_flash_trade = True
    
    # Rule 2: Same Token Swap
    is_same_token = False
    if buySymbol is not None and sellSymbol is not None and buySymbol == sellSymbol:
        is_same_token = True
    
    # Compile results
    result["is_flash_trade"] = is_flash_trade
    result["is_same_token"] = is_same_token
    
    # Calculate overall fraud score - explicitly include is_fraud field
    # is_fraud is REQUIRED in the BigQuery schema, so it should never be null
    result["is_fraud"] = is_flash_trade or is_same_token
    
    # Return result as a map for flexibility
    return result

# Original evaluation function (keeping for backward compatibility)
def evaluate_trade(buyAmount):
    threshold = 0.01
    if buyAmount is None:
        return "Invalid"
    return "Valid" if buyAmount >= threshold else "Invalid"

# Register UDFs
evaluate_trade_udf = udf(evaluate_trade, StringType())
evaluate_fraud_udf = udf(evaluate_fraud, MapType(StringType(), BooleanType()))

def main():
    # Check if enough arguments are provided
    if len(sys.argv) < 3:
        logger.info("Usage: spark-job.py <input_file_path> <bq_table>")
        sys.exit(1)
    
    # Get command line arguments
    input_file_path = sys.argv[1]  # Full GCS path including gs:// prefix
    bq_table = sys.argv[2]  # In format project_id.dataset.table_name
    
    logger.info(f"Processing file: {input_file_path}")
    logger.info(f"Output BigQuery table: {bq_table}")
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("BatchProcessingPipeline").getOrCreate()
    
    # Read JSON file
    raw_df = spark.read.json(input_file_path)
    
    # Print schema to understand structure
    logger.info("Raw data schema:")
    raw_df.printSchema()
    
    # For debugging: show a sample of the raw data
    logger.info("Sample of raw data:")
    raw_df.show(2, truncate=False)
    
    # The correct path to the trades array is data.ethereum.dexTrades
    trades_df = raw_df.select(explode(col("data.ethereum.dexTrades")).alias("trade"))
    
    # Extract and structure the fields according to the BigQuery schema
    processed_df = trades_df.select(
        col("trade.protocol").alias("protocol"),
        struct(
            struct(
                col("trade.block.timestamp.time").alias("time"),
                col("trade.block.timestamp.unixtime").alias("unixtime")
            ).alias("timestamp"),
            col("trade.block.height").alias("height")
        ).alias("block"),
        struct(
            col("trade.transaction.hash").alias("hash")
        ).alias("transaction"),
        struct(
            col("trade.buyCurrency.symbol").alias("symbol"),
            col("trade.buyCurrency.address").alias("address")
        ).alias("buyCurrency"),
        col("trade.buyAmount").alias("buyAmount"),
        struct(
            col("trade.sellCurrency.symbol").alias("symbol"),
            col("trade.sellCurrency.address").alias("address")
        ).alias("sellCurrency"),
        col("trade.sellAmount").alias("sellAmount"),
        col("trade.trades").alias("trades"),
        # Using transaction hash as a proxy for trader address since sender is not available
        col("trade.transaction.hash").alias("trader_address")
    )
    
    # Apply original evaluation based on buy_amount
    df_with_basic = processed_df.withColumn("evaluation",
        evaluate_trade_udf(col("buyAmount")))
    
    # Apply fraud detection rules
    df_with_fraud_map = df_with_basic.withColumn("fraud_detection",
        evaluate_fraud_udf(
            col("buyAmount"),
            col("sellAmount"),
            col("buyCurrency.symbol"),
            col("sellCurrency.symbol"),
            col("transaction.hash")
        )
    )
    
    # Extract the boolean values from the fraud_detection map to separate columns
    # to match the BigQuery schema
    df_with_fraud = df_with_fraud_map.withColumn(
        "is_flash_trade", 
        col("fraud_detection")["is_flash_trade"]
    ).withColumn(
        "is_same_token", 
        col("fraud_detection")["is_same_token"]
    ).withColumn(
        "is_fraud",
        col("fraud_detection")["is_fraud"]
    )
    
    # Ensure all boolean fields have non-null values, especially is_fraud which is REQUIRED
    df_with_fraud = df_with_fraud.na.fill(False, ["is_flash_trade", "is_same_token", "is_fraud"])
    
    # Rule 3: Rapid Transaction detection
    # We need to query historical data from BigQuery for proper window analysis
    
    # First, determine the earliest and latest timestamps in our current batch
    df_with_timestamps = df_with_fraud.withColumn(
        "timestamp_unix", col("block.timestamp.unixtime")
    )
    
    # Show schema for debugging
    logger.info("Schema with timestamp fields:")
    df_with_timestamps.printSchema()
    
    # Collect min/max timestamps (for efficiency, use sampling for large datasets)
    min_ts_row = df_with_timestamps.select("timestamp_unix").orderBy("timestamp_unix").limit(1).collect()
    max_ts_row = df_with_timestamps.select("timestamp_unix").orderBy(col("timestamp_unix").desc()).limit(1).collect()
    
    if min_ts_row and max_ts_row and len(min_ts_row) > 0 and len(max_ts_row) > 0:
        min_timestamp = min_ts_row[0]["timestamp_unix"]
        max_timestamp = max_ts_row[0]["timestamp_unix"]
        
        # Look back 3 minutes (180 seconds) from the earliest timestamp in our data
        lookback_timestamp = min_timestamp - 180
        
        logger.info(f"Analyzing window from {datetime.fromtimestamp(lookback_timestamp)} to {datetime.fromtimestamp(max_timestamp)}")
        
        # Query BigQuery for transactions in our time window
        # Handle both formats: "project.dataset.table" or "project:dataset.table"
        if ":" in bq_table:
            # Format is project:dataset.table
            project_id, rest = bq_table.split(":", 1)
            dataset_id, table_name = rest.split(".", 1)
            bq_parts = [project_id, dataset_id, table_name]
        else:
            # Format is project.dataset.table
            bq_parts = bq_table.split(".")
            
        if len(bq_parts) == 3:
            project_id, dataset_id, table_name = bq_parts
            
            # Build BigQuery options
            bigquery_options = {
                "project": project_id,
                "dataset": dataset_id,
                "table": table_name,
                "filter": f"block.timestamp.unixtime >= {lookback_timestamp} AND block.timestamp.unixtime <= {max_timestamp}"
            }
            
            logger.info(f"Querying BigQuery for historical transactions: {bigquery_options}")
            
            # Query BQ for historical transactions
            try:
                historical_df = spark.read.format("bigquery") \
                    .option("filter", bigquery_options["filter"]) \
                    .option("project", bigquery_options["project"]) \
                    .option("dataset", bigquery_options["dataset"]) \
                    .option("table", bigquery_options["table"]) \
                    .load()
                
                # Select only the columns we need for the rapid transaction detection
                historical_transactions = historical_df.select(
                    col("trader_address"),
                    col("transaction.hash").alias("tx_hash"),
                    col("block.timestamp.unixtime").alias("tx_timestamp")
                )
                
                # Union with the current batch data
                current_transactions = df_with_timestamps.select(
                    col("trader_address"),
                    col("transaction.hash").alias("tx_hash"),
                    col("timestamp_unix").alias("tx_timestamp")
                )
                
                # Tag the source of each transaction to differentiate historical vs. new data
                historical_transactions = historical_transactions.withColumn("data_source", lit("historical"))
                current_transactions = current_transactions.withColumn("data_source", lit("current_batch"))
                
                all_transactions = historical_transactions.union(current_transactions)
                
                # Define window for counting transactions
                windowSpec = Window.partitionBy("trader_address") \
                    .orderBy("tx_timestamp") \
                    .rangeBetween(-180, 0)  # 3-minute window (180 seconds)
                
                # Count transactions per trader within the window
                transaction_counts = all_transactions.withColumn(
                    "transactions_in_window",
                    count("tx_hash").over(windowSpec)
                ).select(
                    "trader_address",
                    "tx_hash", 
                    "transactions_in_window",
                    "data_source"
                )
                
                # Join counts back to the original dataframe
                transaction_counts = transaction_counts.select(
                    "trader_address",
                    "tx_hash",
                    "transactions_in_window", 
                    "data_source"
                )
                
                # Only join with records from the current batch to avoid re-inserting historical data
                df_processed = df_with_timestamps.join(
                    transaction_counts.filter(col("data_source") == "current_batch"),
                    (df_with_timestamps.trader_address == transaction_counts.trader_address) &
                    (df_with_timestamps.transaction.hash == transaction_counts.tx_hash),
                    "left"
                ).withColumn(
                    "is_rapid_transaction",
                    when(col("transactions_in_window") > 3, True).otherwise(False)  # Threshold: more than 3 transactions in 3 minutes
                ).drop(transaction_counts.trader_address).drop(transaction_counts.tx_hash)
                
                # Ensure is_rapid_transaction is never null
                df_processed = df_processed.na.fill(False, ["is_rapid_transaction"])
                
                logger.info("Successfully joined historical transaction data")
                
            except Exception as e:
                logger.error(f"Error querying BigQuery for historical data: {str(e)}")
                # Fallback to just using current batch data if BigQuery query fails
                windowSpec = Window.partitionBy("trader_address") \
                    .orderBy("timestamp_unix") \
                    .rangeBetween(-180, 0)  # 3-minute window (180 seconds)
                
                df_processed = df_with_timestamps.withColumn(
                    "transactions_in_window",
                    count("transaction.hash").over(windowSpec)
                ).withColumn(
                    "is_rapid_transaction",
                    when(col("transactions_in_window") > 3, True).otherwise(False)  # Threshold: more than 3 transactions in 3 minutes
                )
                
                # Ensure is_rapid_transaction is never null
                df_processed = df_processed.na.fill(False, ["is_rapid_transaction"])
                
                logger.warning("Falling back to current batch only for rapid transaction detection")
        else:
            logger.error(f"Invalid BigQuery table format: {bq_table}. Expected format: project.dataset.table or project:dataset.table")
            # Fallback to just using current batch data
            windowSpec = Window.partitionBy("trader_address") \
                .orderBy("timestamp_unix") \
                .rangeBetween(-180, 0)
            
            df_processed = df_with_timestamps.withColumn(
                "transactions_in_window",
                count("transaction.hash").over(windowSpec)
            ).withColumn(
                "is_rapid_transaction",
                when(col("transactions_in_window") > 3, True).otherwise(False)
            )
            
            # Ensure is_rapid_transaction is never null
            df_processed = df_processed.na.fill(False, ["is_rapid_transaction"])
    else:
        logger.warning("No timestamp data available in the batch, skipping rapid transaction detection")
        df_processed = df_with_timestamps.withColumn("transactions_in_window", lit(1)) \
            .withColumn("is_rapid_transaction", lit(False))
    
    # Select the final columns in the order expected by BigQuery
    # Drop the fraud_detection map and timestamp_unix which were used for internal processing
    final_df = df_processed.select(
        "protocol",
        "block",
        "transaction",
        "buyCurrency",
        "buyAmount",
        "sellCurrency",
        "sellAmount",
        "trades",
        "trader_address",
        "evaluation",
        "transactions_in_window",
        "is_rapid_transaction",
        "is_flash_trade",
        "is_same_token", 
        "is_fraud"
    )
    
    # Ensure no nulls in is_fraud field (which is REQUIRED)
    final_df = final_df.na.fill(False, ["is_fraud"])
    
    # Ensure all REQUIRED fields are not null
    # First check for null values in required fields
    for column in ["block", "transaction", "buyCurrency", "is_fraud"]:
        if "." not in column:  # Skip nested fields like block.timestamp
            null_count = final_df.filter(col(column).isNull()).count()
            if null_count > 0:
                logger.warning(f"Found {null_count} NULL values in REQUIRED field {column}")
                
    # Filter out rows with nulls in REQUIRED fields
    final_df = final_df.filter(
        col("block").isNotNull() &
        col("transaction").isNotNull() &
        col("buyCurrency").isNotNull() &
        col("is_fraud").isNotNull()
    )
    
    # Show the processed data
    logger.info("Processed data sample:")
    final_df.show(5, truncate=False)
    
    # Write the results to BigQuery table
    logger.info(f"Writing results to BigQuery table: {bq_table}")
    
    # Replace colon with dot if present for BigQuery connector compatibility
    bq_table_formatted = bq_table.replace(":", ".")
    
    final_df.write.format("bigquery") \
        .option("table", bq_table_formatted) \
        .option("temporaryGcsBucket","gs://defi-sentinal-batch-processing-bucket/tmp/") \
        .mode("append") \
        .save()
    
    logger.info("Processing complete")
    spark.stop()

if __name__ == "__main__":
    main()