import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import json

# logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--inputTopic',
            type=str,
            required=True,
            help='Input Pub/Sub topic')
        parser.add_value_provider_argument(
            '--outputTable',
            type=str,
            required=True,
            help='Output BigQuery table in dataset.table format')

class ExtractFeatures(beam.DoFn):
    def setup(self):
        import numpy as np
        import json
        import logging
        from datetime import datetime
        
        self.np = np
        self.json = json
        self.datetime = datetime
        self.logger = logging.getLogger('ExtractFeatures')
    
    def process(self, trade):
        try:
            # Log the incoming trade for debugging
            self.logger.info(f"Processing trade with hash: {trade.get('transaction', {}).get('hash', 'unknown')}")
            
            # Extract timestamps - use only flattened fields
            timestamp_str = trade['block']['timestamp']['time']
            unix_time = trade['block']['timestamp']['unixtime']
            
            # Extract currency information
            buy_currency = trade['buyCurrency']['symbol']
            buy_address = trade['buyCurrency']['address']
            buy_amount = float(trade['buyAmount'])
            
            sell_currency = trade['sellCurrency']['symbol']
            sell_address = trade['sellCurrency']['address']
            sell_amount = float(trade['sellAmount'])
            
            # Extract transaction details
            tx_hash = trade['transaction']['hash']
            protocol = trade['protocol']
            gas_value = float(trade['gasValue'])
            gas_price = float(trade['gasPrice'])
            
            # Calculate derived features
            trade_ratio = sell_amount / buy_amount if buy_amount > 0 else 0
            gas_cost_relative = gas_value / buy_amount if buy_amount > 0 else 0
            
            # Calculate time features, but convert to Python float
            time_sin = float(self.np.sin(2 * self.np.pi * (unix_time % 86400) / 86400))
            time_cos = float(self.np.cos(2 * self.np.pi * (unix_time % 86400) / 86400))
            
            # Generate a unique ID for deduplication (combines tx hash and timestamp)
            unique_id = f"{tx_hash}_{unix_time}"
            
            # Create feature dictionary - only use flattened fields
            features = {
                'protocol': protocol,
                'block_timestamp': timestamp_str,
                'block_unixtime': unix_time,
                'block_height': trade['block']['height'],
                'tx_hash': tx_hash,
                'buy_currency': buy_currency,
                'buy_address': buy_address,
                'buy_amount': buy_amount,
                'sell_currency': sell_currency,
                'sell_address': sell_address,
                'sell_amount': sell_amount,
                'trade_ratio': trade_ratio,
                'gas_value': gas_value,
                'gas_price': gas_price,
                'gas_cost_relative': gas_cost_relative,
                'time_of_day_sin': time_sin,
                'time_of_day_cos': time_cos,
                'analyzed': False,
                'unique_id': unique_id  # Add the unique ID
            }
            
            self.logger.info(f"Successfully extracted features for tx: {tx_hash}")
            return [features]  # Return a list containing the features dictionary
            
        except (KeyError, ValueError, TypeError, ZeroDivisionError) as e:
            self.logger.error(f"Error extracting features from trade: {str(e)}, trade data: {json.dumps(trade)[:300]}")
            return []  # Return empty list on error

class DetectAnomalies(beam.DoFn):
    def __init__(self):
        self.model = None
        self.scaler = None
        self.window_size = 100  # Smaller window size due to DEX trade frequency
    
    def setup(self):
        import numpy as np
        import logging
        from sklearn.ensemble import IsolationForest
        from sklearn.preprocessing import StandardScaler
        from datetime import datetime
        
        self.np = np
        self.datetime = datetime
        self.logger = logging.getLogger('DetectAnomalies')
        
        self.scaler = StandardScaler()
        # Configure isolation forest for anomaly detection
        self.model = IsolationForest(
            n_estimators=100,
            max_samples='auto',
            contamination=0.05,  # Expect 5% of trades to be anomalous
            max_features=1.0,
            bootstrap=False,
            n_jobs=-1,
            random_state=42
        )
        
        self.logger.info("DetectAnomalies setup complete with Isolation Forest model")
    
    def _convert_numpy_types(self, obj):
        """Convert numpy types to Python native types for JSON serialization"""
        try:
            if isinstance(obj, self.np.integer):
                return int(obj)
            elif isinstance(obj, self.np.floating):
                return float(obj)
            elif isinstance(obj, self.np.ndarray):
                return obj.tolist()
            elif isinstance(obj, dict):
                return {key: self._convert_numpy_types(value) for key, value in obj.items()}
            elif isinstance(obj, list):
                return [self._convert_numpy_types(item) for item in obj]
            else:
                return obj
        except Exception as e:
            self.logger.error(f"Error converting NumPy types: {str(e)}")
            return obj  # Return original object if conversion fails
    
    def process(self, window_data):
        # Log the incoming window size
        self.logger.info(f"Received window with {len(window_data)} trades")
        
        # Skip if we don't have enough data
        if len(window_data) < 10:  # Need at least 10 samples
            self.logger.info(f"Not enough data for anomaly detection: {len(window_data)} trades")
            # Return all the trades without anomaly detection
            all_results = []
            for trade in window_data:
                result = trade.copy()
                result['anomaly_score'] = 0.0
                result['detection_time'] = self.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                result['reason'] = "Insufficient data for analysis"
                all_results.append(result)
            return all_results
        
        try:
            # Log tx hashes in this window
            tx_hashes = [tx.get('tx_hash', 'unknown')[:10] for tx in window_data[:5]]
            self.logger.info(f"Processing window with transactions: {tx_hashes}...")
            
            # Extract feature matrix - select numerical features for model
            feature_matrix = self.np.array([
                [
                    tx['buy_amount'],
                    tx['sell_amount'],
                    tx['trade_ratio'],
                    tx['gas_value'],
                    tx['gas_price'],
                    tx['gas_cost_relative'],
                    tx['time_of_day_sin'],
                    tx['time_of_day_cos']
                ]
                for tx in window_data
            ])
            
            # Handle potential NaN or infinite values
            feature_matrix = self.np.nan_to_num(feature_matrix, nan=0.0, posinf=1e10, neginf=-1e10)
            
            # Log feature matrix stats
            self.logger.info(f"Feature matrix shape: {feature_matrix.shape}")
            
            # Scale features
            scaled_features = self.scaler.fit_transform(feature_matrix)
            
            # Train model on current window
            self.model.fit(scaled_features)
            self.logger.info("Isolation Forest model trained on current window")
            
            # Predict anomalies
            predictions = self.model.predict(scaled_features)
            scores = self.model.decision_function(scaled_features)
            
            # Count anomalies
            anomaly_count = int(self.np.sum(predictions == -1))
            self.logger.info(f"Detected {anomaly_count} anomalies out of {len(window_data)} trades")
            
            # Create a list to collect all results
            all_results = []
            
            # Process all trades in window (both normal and anomalous)
            for i, pred in enumerate(predictions):
                result = window_data[i].copy()
        
                # Add anomaly score and detection time for ALL trades
                result['anomaly_score'] = float(scores[i])
                result['detection_time'] = self.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if pred == -1:
                    # This is an anomalous trade
                    result_tx_hash = result.get('tx_hash', 'unknown')
                    self.logger.info(f"Found anomalous transaction: {result_tx_hash}")
                    
                    # [Your existing anomaly reason logic]
                    feature_names = ['buy_amount', 'sell_amount', 'trade_ratio', 'gas_value', 
                                    'gas_price', 'gas_cost_relative', 'time_of_day_sin', 'time_of_day_cos']
                    
                    # Find the most anomalous feature
                    feature_values = feature_matrix[i]
                    scaled_values = scaled_features[i]
                    max_idx = int(self.np.argmax(self.np.abs(scaled_values)))
                    
                    if max_idx < len(feature_names):
                        primary_factor = feature_names[max_idx]
                        
                        # Create reason based on primary anomalous factor
                        if primary_factor in ['buy_amount', 'sell_amount']:
                            if feature_values[max_idx] > 0:
                                result['reason'] = f"Unusually large {primary_factor}"
                            else:
                                result['reason'] = f"Unusually small {primary_factor}"
                        elif primary_factor == 'trade_ratio':
                            result['reason'] = "Unusual price ratio between tokens"
                        elif primary_factor in ['gas_value', 'gas_price']:
                            result['reason'] = "Unusual gas parameters"
                        elif primary_factor == 'gas_cost_relative':
                            result['reason'] = "Gas cost disproportionate to transaction size"
                        elif primary_factor in ['time_of_day_sin', 'time_of_day_cos']:
                            result['reason'] = "Transaction at unusual time of day"
                        else:
                            result['reason'] = "Multiple anomalous factors"
                    else:
                        result['reason'] = "Unknown anomalous factor"
                    
                    self.logger.info(f"Anomaly reason: {result['reason']}, score: {result['anomaly_score']}")
                else:
                    # This is a normal trade
                    result['reason'] = "No anomaly detected"
                    self.logger.info(f"Normal transaction: {result.get('tx_hash', 'unknown')}, score: {result['anomaly_score']}")
                
                # Convert all numpy types to Python native types
                result = self._convert_numpy_types(result)
                
                # Add the result to our collection
                all_results.append(result)
                    
            # Return all results as a single list
            self.logger.info(f"Result length size: {len(all_results)}")
            return all_results
                    
        except Exception as e:
            import traceback
            trace = traceback.format_exc()
            self.logger.error(f"Error in anomaly detection: {str(e)}")
            self.logger.error(f"Stack trace: {trace}")
            
            # Return all trades with an error status
            all_results = []
            for trade in window_data:
                result = trade.copy()
                result['anomaly_score'] = 0.0
                result['detection_time'] = self.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                result['reason'] = f"Error in anomaly detection: {str(e)}"
                all_results.append(result)
            return all_results


def json_decoder(message):
    """Decode JSON message from PubSub"""
    import json
    import logging
    
    logger = logging.getLogger('json_decoder')
    try:
        # Log the message length for debugging
        logger.info(f"Received message of length: {len(message)}")
        decoded = json.loads(message)
        logger.info(f"Successfully decoded JSON message")
        return decoded
    except Exception as e:
        logger.error(f"Error decoding JSON: {str(e)}")
        return {}

def extract_dex_trades(message):
    """Extract dexTrades from message"""
    import logging
    
    logger = logging.getLogger('extract_dex_trades')
    try:
        trades = message.get("data", {}).get("ethereum", {}).get("dexTrades", [])
        logger.info(f"Extracted {len(trades)} trades from message")
        return trades
    except Exception as e:
        logger.error(f"Error extracting dexTrades: {str(e)}")
        return []

def run():
    import json
    import logging
    
    logger = logging.getLogger('run')
    logger.info("Starting DEX trade anomaly detection pipeline")
    
    pipeline_options = PipelineOptions(streaming=True)
    user_options = pipeline_options.view_as(UserOptions)
    
    logger.info("Pipeline options created")
    
    with beam.Pipeline(options=pipeline_options) as p:
        logger.info("Pipeline created")
        
        # Read messages from PubSub
        dex_trades = (
            p 
            | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=user_options.inputTopic.get())
            | "Decode JSON" >> beam.Map(json_decoder)
            | "Extract dexTrades" >> beam.FlatMap(extract_dex_trades)
        )
        
        logger.info("PubSub reading step configured")
        
        # Extract features from trades (now includes generating unique IDs)
        features = (
            dex_trades
            | "Extract Features" >> beam.ParDo(ExtractFeatures())
        )
        
        logger.info("Feature extraction step configured")
        
        # 1) Window + dedupe all in one shot
        deduped_in_window = (
            features
            | "Fixed Window 1m" >> beam.WindowInto(
                beam.window.FixedWindows(60),
                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING
            )
            | "Key By UniqueId" >> beam.Map(lambda x: (x['unique_id'], x))
            | "Group By UniqueId" >> beam.GroupByKey()
            | "Take First Per Key" >> beam.MapTuple(lambda _id, recs: recs[0])
        )

        # 2) Within that same window, collect all deduped records, then detect anomalies
        analyzed_trades = (
            deduped_in_window
            | "Group All Trades" >> beam.GroupBy(lambda x: "all")
            | "Extract Trade Lists" >> beam.MapTuple(lambda _, trades: trades)
            | "Detect Anomalies"   >> beam.ParDo(DetectAnomalies())
            | "Flatten Results"    >> beam.FlatMap(lambda x: x if isinstance(x, list) else [x])
        )
        
        logger.info("Anomaly detection step configured")
        
        # We no longer need to add insertId for BigQuery deduplication since we're using Beam's Distinct
        
        # Write ALL trades to BigQuery
        from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
        bq_schema = parse_table_schema_from_json(json.dumps({
            "fields": [
                {"name": "protocol", "type": "STRING"},
                {"name": "block_timestamp", "type": "TIMESTAMP"},
                {"name": "block_unixtime", "type": "INTEGER"},
                {"name": "block_height", "type": "INTEGER"},
                {"name": "tx_hash", "type": "STRING"},
                {"name": "buy_currency", "type": "STRING"},
                {"name": "buy_address", "type": "STRING"},
                {"name": "buy_amount", "type": "FLOAT"},
                {"name": "sell_currency", "type": "STRING"},
                {"name": "sell_address", "type": "STRING"},
                {"name": "sell_amount", "type": "FLOAT"},
                {"name": "trade_ratio", "type": "FLOAT"},
                {"name": "gas_value", "type": "FLOAT"},
                {"name": "gas_price", "type": "FLOAT"},
                {"name": "gas_cost_relative", "type": "FLOAT"},
                {"name": "time_of_day_sin", "type": "FLOAT"},
                {"name": "time_of_day_cos", "type": "FLOAT"},
                {"name": "anomaly_score", "type": "FLOAT"},
                {"name": "detection_time", "type": "TIMESTAMP"},
                {"name": "reason", "type": "STRING"},
                {"name": "analyzed", "type": "BOOLEAN"}
            ]
        }))
        
        # We still generate unique_id for deduplication purposes but remove it before writing to BigQuery
        bq_rows = (
            analyzed_trades
            | "Remove unique_id field" >> beam.Map(lambda x: {k: v for k, v in x.items() if k != 'unique_id'})
            | "Log Before Write" >> beam.Map(lambda x: logger.info(f"Writing trade to BigQuery: {x.get('tx_hash', 'unknown')}") or x)
        )
        
        # Write to BigQuery (after removing unique_id)
        trades_writing = (
            bq_rows
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                user_options.outputTable.get(),
                schema=bq_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters={'ignoreUnknownValues': True}
            )
        )


if __name__ == "__main__":
    run()