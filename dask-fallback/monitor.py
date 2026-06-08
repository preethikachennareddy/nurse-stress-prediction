"""
Monitoring service that checks Flink/Spark processing latency
and triggers Dask fallback when resource constraints are detected.
"""
import time
import logging
import requests
import yaml
from influxdb import InfluxDBClient
from datetime import datetime, timedelta
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LatencyMonitor:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.influx_client = InfluxDBClient(
            host=self.config['influxdb']['url'].replace('http://', '').split(':')[0],
            port=int(self.config['influxdb']['url'].split(':')[-1]),
            database=self.config['influxdb']['database']
        )
        
        self.latency_threshold = self.config['monitoring']['latency_threshold_seconds']
        self.consecutive_violations = self.config['monitoring']['consecutive_violations']
        self.check_interval = self.config['monitoring']['check_interval_seconds']
        
        self.violation_count = 0
        self.fallback_active = False
        
    def get_processing_latency(self) -> Optional[float]:
        """
        Query InfluxDB to get the current processing latency.
        Latency = current_time - latest_processed_event_time
        """
        try:
            query = f"""
            SELECT last("datetime") as last_processed
            FROM "{self.config['influxdb']['measurement']}"
            WHERE time > now() - 10m
            """
            result = self.influx_client.query(query)
            
            if not result:
                logger.warning("No recent data found in InfluxDB")
                return None
            
            points = list(result.get_points())
            if not points:
                return None
                
            last_processed_ts = points[0]['last_processed']
            current_ts = time.time() * 1000  # Convert to milliseconds
            
            latency_ms = current_ts - last_processed_ts
            latency_seconds = latency_ms / 1000.0
            
            logger.info(f"Current processing latency: {latency_seconds:.2f}s")
            return latency_seconds
            
        except Exception as e:
            logger.error(f"Error querying InfluxDB: {e}")
            return None
    
    def check_flink_health(self) -> bool:
        """
        Check if Flink is running and healthy.
        Returns True if healthy, False otherwise.
        """
        try:
            response = requests.get("http://jobmanager:8081/overview", timeout=5)
            if response.status_code == 200:
                data = response.json()
                # Check if there are running jobs
                if data.get('jobs-running', 0) > 0:
                    return True
            return False
        except Exception as e:
            logger.warning(f"Flink health check failed: {e}")
            return False
    
    def should_trigger_fallback(self) -> bool:
        """
        Determine if fallback should be triggered based on latency and health checks.
        """
        # Check if Flink is healthy
        if not self.check_flink_health():
            logger.warning("Flink is not healthy")
            self.violation_count += 1
        else:
            # Check latency
            latency = self.get_processing_latency()
            
            if latency is None:
                # No data - might be starting up
                return False
            
            if latency > self.latency_threshold:
                self.violation_count += 1
                logger.warning(
                    f"Latency violation {self.violation_count}/{self.consecutive_violations}: "
                    f"{latency:.2f}s > {self.latency_threshold}s"
                )
            else:
                # Reset counter if latency is acceptable
                if self.violation_count > 0:
                    logger.info("Latency back to normal, resetting violation count")
                self.violation_count = 0
        
        # Trigger fallback if we have consecutive violations
        if self.violation_count >= self.consecutive_violations:
            return True
        
        return False
    
    def should_resume_primary(self) -> bool:
        """
        Check if we should switch back to primary (Flink) processing.
        """
        if not self.fallback_active:
            return False
        
        # Check if Flink is healthy and latency is acceptable
        if self.check_flink_health():
            latency = self.get_processing_latency()
            if latency and latency < self.latency_threshold * 0.5:  # 50% threshold for hysteresis
                logger.info("Primary processing path is healthy, can resume")
                return True
        
        return False
    
    def run(self, on_fallback_trigger, on_primary_resume):
        """
        Main monitoring loop.
        
        Args:
            on_fallback_trigger: Callback function to trigger fallback
            on_primary_resume: Callback function to resume primary processing
        """
        logger.info("Starting latency monitor...")
        
        while True:
            try:
                if not self.fallback_active:
                    # Monitor for fallback trigger
                    if self.should_trigger_fallback():
                        logger.critical("TRIGGERING DASK FALLBACK MODE")
                        self.fallback_active = True
                        self.violation_count = 0
                        on_fallback_trigger()
                else:
                    # Monitor for primary resume
                    if self.should_resume_primary():
                        logger.info("RESUMING PRIMARY PROCESSING MODE")
                        self.fallback_active = False
                        on_primary_resume()
                
                time.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("Monitor stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(self.check_interval)


if __name__ == "__main__":
    def trigger_fallback():
        logger.info("Fallback triggered - start Dask processor")
        # In production, this would signal the orchestrator
    
    def resume_primary():
        logger.info("Primary resumed - stop Dask processor")
        # In production, this would signal the orchestrator
    
    monitor = LatencyMonitor()
    monitor.run(trigger_fallback, resume_primary)
