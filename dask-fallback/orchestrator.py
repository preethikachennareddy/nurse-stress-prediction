"""
Orchestrator that manages the switching between Flink/Spark and Dask processing.
Monitors system health and coordinates the fallback mechanism.
"""
import logging
import threading
import signal
import sys
from monitor import LatencyMonitor
from dask_processor import DaskStressProcessor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProcessingOrchestrator:
    def __init__(self):
        self.monitor = LatencyMonitor()
        self.dask_processor = None
        self.dask_thread = None
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, shutting down...")
        self.running = False
        self.stop_dask_processor()
        sys.exit(0)
    
    def start_dask_processor(self):
        """Start the Dask fallback processor in a separate thread."""
        if self.dask_processor and self.dask_thread and self.dask_thread.is_alive():
            logger.warning("Dask processor is already running")
            return
        
        logger.info("Starting Dask fallback processor...")
        
        self.dask_processor = DaskStressProcessor()
        self.dask_thread = threading.Thread(
            target=self.dask_processor.run,
            daemon=True
        )
        self.dask_thread.start()
        
        logger.info("Dask processor started successfully")
    
    def stop_dask_processor(self):
        """Stop the Dask fallback processor."""
        if not self.dask_processor:
            logger.info("Dask processor is not running")
            return
        
        logger.info("Stopping Dask fallback processor...")
        
        if self.dask_processor:
            self.dask_processor.stop()
        
        if self.dask_thread and self.dask_thread.is_alive():
            self.dask_thread.join(timeout=10)
        
        self.dask_processor = None
        self.dask_thread = None
        
        logger.info("Dask processor stopped successfully")
    
    def on_fallback_trigger(self):
        """
        Callback when fallback is triggered.
        Starts Dask processor to handle the load.
        """
        logger.critical("=" * 60)
        logger.critical("FALLBACK TRIGGERED: Switching to Dask processing")
        logger.critical("=" * 60)
        
        self.start_dask_processor()
        
        # Note: We don't stop Flink here - both can run concurrently
        # Dask will consume from the latest checkpoint, ensuring no data loss
        logger.info("Dask is now processing alongside primary path")
    
    def on_primary_resume(self):
        """
        Callback when primary processing is healthy again.
        Stops Dask processor and returns to Flink/Spark only.
        """
        logger.info("=" * 60)
        logger.info("PRIMARY RESUMED: Stopping Dask fallback")
        logger.info("=" * 60)
        
        self.stop_dask_processor()
        
        logger.info("Returned to primary processing path (Flink/Spark)")
    
    def run(self):
        """
        Main orchestration loop.
        Monitors system health and coordinates processing modes.
        """
        logger.info("=" * 60)
        logger.info("STRESS PREDICTION ORCHESTRATOR STARTED")
        logger.info("=" * 60)
        logger.info("Monitoring Flink/Spark processing health...")
        logger.info("Will trigger Dask fallback if latency exceeds threshold")
        logger.info("")
        
        try:
            # Start the monitor with callbacks
            self.monitor.run(
                on_fallback_trigger=self.on_fallback_trigger,
                on_primary_resume=self.on_primary_resume
            )
        except KeyboardInterrupt:
            logger.info("Orchestrator stopped by user")
        except Exception as e:
            logger.error(f"Error in orchestrator: {e}", exc_info=True)
        finally:
            self.stop_dask_processor()
            logger.info("Orchestrator shutdown complete")


if __name__ == "__main__":
    orchestrator = ProcessingOrchestrator()
    orchestrator.run()
