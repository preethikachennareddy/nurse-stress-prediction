"""
Dask-based fallback processor for stress prediction.
Processes larger batches with lower frequency when Flink/Spark is overloaded.
"""
import time
import logging
import yaml
import json
import requests
import avro.schema
import avro.io
from io import BytesIO
from kafka import KafkaConsumer
from influxdb import InfluxDBClient
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from typing import List, Dict, Optional
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DaskStressProcessor:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.config['kafka']['topic'],
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            group_id=self.config['kafka']['group_id'],
            auto_offset_reset=self.config['kafka']['auto_offset_reset'],
            enable_auto_commit=self.config['kafka']['enable_auto_commit'],
            auto_commit_interval_ms=self.config['kafka']['auto_commit_interval_ms'],
            value_deserializer=lambda m: m  # Raw bytes, will decode Avro manually
        )
        
        # Load Avro schema
        schema_path = "/data/nurse_sensor_event.avsc"
        self.avro_schema = avro.schema.parse(open(schema_path, "r").read())
        
        # Initialize InfluxDB client
        influx_url = self.config['influxdb']['url']
        host = influx_url.replace('http://', '').split(':')[0]
        port = int(influx_url.split(':')[-1])
        
        self.influx_client = InfluxDBClient(
            host=host,
            port=port,
            database=self.config['influxdb']['database']
        )
        
        # Initialize Dask
        self.dask_client = self._init_dask()
        
        self.batch_size = self.config['processing']['batch_size']
        self.batch_timeout = self.config['processing']['batch_timeout_seconds']
        self.checkpoint_interval = self.config['processing']['checkpoint_interval']
        
        self.running = False
        self.batch_count = 0
        
    def _init_dask(self) -> Optional[Client]:
        """Initialize Dask client (local or distributed)."""
        try:
            scheduler_addr = self.config['dask']['scheduler_address']
            
            if scheduler_addr:
                # Connect to existing Dask cluster
                logger.info(f"Connecting to Dask scheduler at {scheduler_addr}")
                client = Client(scheduler_addr)
            else:
                # Create local cluster
                logger.info("Starting local Dask cluster")
                cluster = LocalCluster(
                    n_workers=self.config['dask']['n_workers'],
                    threads_per_worker=self.config['dask']['threads_per_worker'],
                    memory_limit=self.config['dask']['memory_limit']
                )
                client = Client(cluster)
            
            logger.info(f"Dask dashboard: {client.dashboard_link}")
            return client
            
        except Exception as e:
            logger.warning(f"Could not initialize Dask cluster: {e}")
            logger.info("Will process without Dask parallelization")
            return None
    
    def decode_avro(self, raw_bytes: bytes) -> Dict:
        """Decode Avro message."""
        bytes_reader = BytesIO(raw_bytes)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(self.avro_schema)
        return reader.read(decoder)
    
    def collect_batch(self) -> List[Dict]:
        """Collect a batch of messages from Kafka."""
        batch = []
        start_time = time.time()
        
        logger.info(f"Collecting batch (target size: {self.batch_size})...")
        
        while len(batch) < self.batch_size:
            # Check timeout
            if time.time() - start_time > self.batch_timeout:
                logger.info(f"Batch timeout reached, processing {len(batch)} messages")
                break
            
            # Poll for messages
            messages = self.consumer.poll(timeout_ms=1000, max_records=100)
            
            if not messages:
                continue
            
            for topic_partition, records in messages.items():
                for record in records:
                    try:
                        decoded = self.decode_avro(record.value)
                        batch.append(decoded)
                        
                        if len(batch) >= self.batch_size:
                            break
                    except Exception as e:
                        logger.error(f"Error decoding message: {e}")
                
                if len(batch) >= self.batch_size:
                    break
        
        logger.info(f"Collected {len(batch)} messages in {time.time() - start_time:.2f}s")
        return batch
    
    def process_batch(self, batch: List[Dict]) -> pd.DataFrame:
        """
        Process a batch of sensor data:
        1. Convert to DataFrame
        2. Group by nurse ID
        3. Calculate averages per nurse
        4. Predict stress levels
        """
        if not batch:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(batch)
        
        # Group by nurse ID and calculate averages
        aggregated = df.groupby('id').agg({
            'X': 'mean',
            'Y': 'mean',
            'Z': 'mean',
            'EDA': 'mean',
            'HR': 'mean',
            'TEMP': 'mean',
            'datetime': 'max'  # Use latest timestamp
        }).reset_index()
        
        logger.info(f"Aggregated {len(df)} records into {len(aggregated)} nurse summaries")
        
        # Predict stress levels
        predictions = []
        for _, row in aggregated.iterrows():
            try:
                stress_level = self.predict_stress(
                    row['X'], row['Y'], row['Z'],
                    row['EDA'], row['HR'], row['TEMP']
                )
                predictions.append({
                    'id': row['id'],
                    'X': row['X'],
                    'Y': row['Y'],
                    'Z': row['Z'],
                    'EDA': row['EDA'],
                    'HR': row['HR'],
                    'TEMP': row['TEMP'],
                    'datetime': row['datetime'],
                    'stress_level': stress_level
                })
            except Exception as e:
                logger.error(f"Error predicting stress for nurse {row['id']}: {e}")
        
        return pd.DataFrame(predictions)
    
    def predict_stress(self, x: float, y: float, z: float, 
                      eda: float, hr: float, temp: float) -> int:
        """Call ML service to predict stress level."""
        endpoint = self.config['ml_service']['endpoint']
        url = f"{endpoint}?x={x}&y={y}&z={z}&eda={eda}&hr={hr}&temp={temp}"
        
        for attempt in range(self.config['ml_service']['retry_attempts']):
            try:
                response = requests.get(
                    url,
                    timeout=self.config['ml_service']['timeout_seconds']
                )
                
                if response.status_code == 200:
                    result = response.json()
                    # Convert to float first, then to int (handles "2.0" string)
                    return int(float(result['stress_level_prediction']))
                else:
                    logger.warning(f"ML service returned {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Error calling ML service (attempt {attempt + 1}): {e}")
                time.sleep(1)
        
        # Default to moderate stress if prediction fails
        logger.warning("Using default stress level (1) due to prediction failure")
        return 1
    
    def write_to_influxdb(self, results: pd.DataFrame):
        """Write results to InfluxDB."""
        if results.empty:
            return
        
        points = []
        for _, row in results.iterrows():
            # Convert datetime string to timestamp in milliseconds
            try:
                dt_str = str(row['datetime'])
                # Parse datetime and convert to milliseconds timestamp
                from datetime import datetime
                dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
                timestamp_ms = int(dt.timestamp() * 1000)
            except Exception as e:
                logger.warning(f"Error parsing datetime '{row['datetime']}': {e}, using current time")
                timestamp_ms = int(time.time() * 1000)
            
            point = {
                "measurement": self.config['influxdb']['measurement'],
                "tags": {
                    "id": str(row['id']),
                    "source": "dask-fallback"
                },
                "time": timestamp_ms,
                "fields": {
                    "X": float(row['X']),
                    "Y": float(row['Y']),
                    "Z": float(row['Z']),
                    "EDA": float(row['EDA']),
                    "HR": float(row['HR']),
                    "TEMP": float(row['TEMP']),
                    "stress_level": int(row['stress_level'])
                }
            }
            points.append(point)
        
        try:
            self.influx_client.write_points(points, time_precision='ms')
            logger.info(f"Wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logger.error(f"Error writing to InfluxDB: {e}")
    
    def run(self):
        """Main processing loop."""
        logger.info("Starting Dask fallback processor...")
        logger.info(f"Batch size: {self.batch_size}, Timeout: {self.batch_timeout}s")
        
        self.running = True
        
        try:
            while self.running:
                # Collect batch from Kafka
                batch = self.collect_batch()
                
                if not batch:
                    logger.info("No messages received, waiting...")
                    time.sleep(5)
                    continue
                
                # Process batch
                start_time = time.time()
                results = self.process_batch(batch)
                processing_time = time.time() - start_time
                
                logger.info(f"Processed batch in {processing_time:.2f}s")
                
                # Write to InfluxDB
                self.write_to_influxdb(results)
                
                # Commit Kafka offsets
                self.consumer.commit()
                
                self.batch_count += 1
                
                if self.batch_count % self.checkpoint_interval == 0:
                    logger.info(f"Checkpoint: Processed {self.batch_count} batches")
                
        except KeyboardInterrupt:
            logger.info("Processor stopped by user")
        except Exception as e:
            logger.error(f"Error in processing loop: {e}", exc_info=True)
        finally:
            self.stop()
    
    def stop(self):
        """Clean shutdown."""
        logger.info("Stopping Dask processor...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
        
        if self.dask_client:
            self.dask_client.close()
        
        logger.info("Dask processor stopped")


if __name__ == "__main__":
    processor = DaskStressProcessor()
    processor.run()
