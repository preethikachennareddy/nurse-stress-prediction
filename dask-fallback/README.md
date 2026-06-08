# Dask Fallback System for Stress Prediction

## Overview

This is the **fallback processing system** that automatically activates when the primary Flink/Spark ML pipeline experiences resource constraints. It ensures continuous stress monitoring without data loss by processing larger batches at lower frequency using Dask.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Latency Monitor                         │   │
│  │  - Checks Flink/Spark health every 60s              │   │
│  │  - Monitors processing latency via InfluxDB         │   │
│  │  - Triggers fallback after 3 consecutive violations │   │
│  └──────────────────────────────────────────────────────┘   │
│                           │                                  │
│              ┌────────────┴────────────┐                    │
│              │                         │                    │
│         HEALTHY                   OVERLOADED                │
│              │                         │                    │
│    ┌─────────▼────────┐      ┌────────▼─────────┐         │
│    │  Flink/Spark     │      │  Flink/Spark +   │         │
│    │  (Primary)       │      │  Dask (Fallback) │         │
│    └──────────────────┘      └──────────────────┘         │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Kafka     │
                    │ (Checkpoints)│
                    └──────────────┘
```

## Key Features

### 1. Automatic Fallback Triggering
- **Monitors** Flink processing latency every 60 seconds
- **Triggers** Dask when latency exceeds 5 minutes for 3 consecutive checks
- **Resumes** primary path when latency drops below 2.5 minutes

### 2. Data Integrity Guarantees
- Uses **Kafka consumer groups** with separate group ID (`dask-fallback-consumer`)
- Starts from **latest checkpoint** when activated (no duplicate processing)
- **Auto-commits** offsets every 5 seconds
- Both Flink and Dask can run **concurrently** without conflicts

### 3. Batch Processing Strategy
- **Batch size**: 1000 messages (vs 60 for Flink)
- **Timeout**: 30 seconds max wait time
- **Aggregation**: Groups by nurse ID, calculates averages
- **Prediction**: Calls same ML service as Flink

### 4. Performance Trade-offs
| Aspect | Flink/Spark | Dask Fallback |
|--------|-------------|---------------|
| Batch Size | 60 messages | 1000 messages |
| Window | 5 seconds | 30 seconds |
| Latency | ~5s | ~30-60s |
| Throughput | High | Similar |
| Resource Usage | High | Lower |

## Components

### 1. `monitor.py` - Latency Monitor
Continuously checks system health and triggers mode switches.

**Key Functions:**
- `get_processing_latency()` - Queries InfluxDB for current lag
- `check_flink_health()` - Verifies Flink is running
- `should_trigger_fallback()` - Decision logic for switching
- `should_resume_primary()` - Decision logic for returning

### 2. `dask_processor.py` - Dask Processor
Handles batch processing when fallback is active.

**Key Functions:**
- `collect_batch()` - Accumulates messages from Kafka
- `process_batch()` - Aggregates by nurse ID, calculates averages
- `predict_stress()` - Calls ML service for predictions
- `write_to_influxdb()` - Writes results with `source=dask-fallback` tag

### 3. `orchestrator.py` - Orchestrator
Coordinates the entire fallback system.

**Key Functions:**
- `start_dask_processor()` - Launches Dask in separate thread
- `stop_dask_processor()` - Gracefully shuts down Dask
- `on_fallback_trigger()` - Callback when fallback activates
- `on_primary_resume()` - Callback when returning to primary

### 4. `config.yaml` - Configuration
All system parameters in one place.

**Key Settings:**
```yaml
monitoring:
  latency_threshold_seconds: 300  # 5 minutes
  consecutive_violations: 3
  check_interval_seconds: 60

processing:
  batch_size: 1000
  batch_timeout_seconds: 30
```

## How to Use

### Option 1: Automatic Mode (Recommended)
The orchestrator runs continuously and handles everything automatically.

```bash
# Start all services including Dask fallback
docker compose up -d

# View orchestrator logs
docker compose logs dask-fallback -f
```

**What happens:**
1. Orchestrator monitors Flink health
2. When latency exceeds threshold → Dask starts automatically
3. When latency normalizes → Dask stops automatically
4. No manual intervention needed

### Option 2: Manual Testing
Test the Dask processor independently.

```bash
# Enter the container
docker compose exec dask-fallback bash

# Run processor directly
python dask_processor.py

# Or run monitor only
python monitor.py
```

### Option 3: Standalone Dask (No Monitoring)
Run Dask processor without automatic switching.

```bash
# Modify docker-compose.yml
dask-fallback:
  command: ["python", "dask_processor.py"]  # Instead of orchestrator.py
```

## Configuration

### Adjusting Thresholds

Edit `dask-fallback/config.yaml`:

```yaml
monitoring:
  latency_threshold_seconds: 300  # Lower = more sensitive
  consecutive_violations: 3        # Higher = less false positives
  check_interval_seconds: 60       # How often to check
```

### Adjusting Batch Size

```yaml
processing:
  batch_size: 1000              # Larger = lower frequency, higher latency
  batch_timeout_seconds: 30     # Max wait time for batch
```

### Using Distributed Dask

For better performance, connect to a Dask cluster:

```yaml
dask:
  scheduler_address: "dask-scheduler:8786"  # Instead of null
```

Then add Dask scheduler/workers to `docker-compose.yml`:

```yaml
dask-scheduler:
  image: daskdev/dask:latest
  command: ["dask-scheduler"]
  ports:
    - "8786:8786"
    - "8787:8787"  # Dashboard
  networks:
    - app-net

dask-worker:
  image: daskdev/dask:latest
  command: ["dask-worker", "dask-scheduler:8786"]
  depends_on:
    - dask-scheduler
  networks:
    - app-net
```

## Monitoring

### Check Orchestrator Status

```bash
# View logs
docker compose logs dask-fallback -f

# Look for these messages:
# "Starting latency monitor..."
# "Current processing latency: X.XXs"
# "TRIGGERING DASK FALLBACK MODE"
# "RESUMING PRIMARY PROCESSING MODE"
```

### Verify Dask is Processing

```bash
# Check InfluxDB for Dask-tagged data
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT * FROM nurse_stress WHERE source='dask-fallback' LIMIT 10"
```

### View Kafka Consumer Groups

```bash
# Check both consumer groups
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --list

# Should show:
# - flink-consumer (primary)
# - dask-fallback-consumer (fallback)
```

## Troubleshooting

### Dask Not Starting

**Symptom:** Orchestrator logs show "TRIGGERING DASK FALLBACK" but no processing

**Solutions:**
1. Check Kafka connectivity:
   ```bash
   docker compose exec dask-fallback python -c "from kafka import KafkaConsumer; print('OK')"
   ```

2. Verify Avro schema is mounted:
   ```bash
   docker compose exec dask-fallback ls -la /data/nurse_sensor_event.avsc
   ```

3. Check ML service is reachable:
   ```bash
   docker compose exec dask-fallback curl http://stress-prediction-service:5000/model/api/predict?x=0&y=0&z=0&eda=0&hr=70&temp=36
   ```

### False Positive Triggers

**Symptom:** Dask starts/stops too frequently

**Solution:** Increase thresholds in `config.yaml`:
```yaml
monitoring:
  consecutive_violations: 5  # Instead of 3
  latency_threshold_seconds: 600  # Instead of 300
```

### High Memory Usage

**Symptom:** Container OOM errors

**Solutions:**
1. Reduce batch size:
   ```yaml
   processing:
     batch_size: 500  # Instead of 1000
   ```

2. Limit Dask memory:
   ```yaml
   dask:
     memory_limit: "1GB"  # Instead of 2GB
   ```

3. Add memory limits to docker-compose:
   ```yaml
   dask-fallback:
     deploy:
       resources:
         limits:
           memory: 2G
   ```

### Data Not Appearing in InfluxDB

**Symptom:** Dask logs show processing but no data in InfluxDB

**Solutions:**
1. Check InfluxDB connection:
   ```bash
   docker compose exec dask-fallback python -c \
     "from influxdb import InfluxDBClient; c = InfluxDBClient('influxdb', 8086); print(c.ping())"
   ```

2. Verify database exists:
   ```bash
   docker compose exec influxdb influx -execute "SHOW DATABASES"
   ```

3. Check for errors in logs:
   ```bash
   docker compose logs dask-fallback | grep -i error
   ```

## Integration with Existing System

### No Conflicts with Flink/Spark
- Uses **separate Kafka consumer group** (`dask-fallback-consumer`)
- Writes to **same InfluxDB** with `source=dask-fallback` tag
- Can run **concurrently** with Flink without interference

### Grafana Visualization
Update Grafana queries to show both sources:

```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE source='flink' OR source='dask-fallback'
GROUP BY time(1m), id, source
```

### Switching Back to Primary
When Dask stops:
- Kafka offsets are committed
- Flink continues from its own checkpoint
- No data loss or duplication
- Seamless transition

## Performance Benchmarks

Based on the project requirements:

| Metric | Flink (Primary) | Dask (Fallback) |
|--------|-----------------|-----------------|
| Messages/sec | ~3,333 (1M/5min) | ~33 (1K/30sec) |
| Latency | 5 seconds | 30-60 seconds |
| Memory | ~2-4 GB | ~1-2 GB |
| CPU | High (continuous) | Medium (batched) |
| Throughput | High | Similar (catches up) |

## Testing the Fallback

### Simulate Resource Constraint

1. **Stop Flink** to simulate overload:
   ```bash
   docker compose stop jobmanager taskmanager
   ```

2. **Wait 3-5 minutes** for monitor to detect issue

3. **Check logs** for fallback trigger:
   ```bash
   docker compose logs dask-fallback -f
   # Should see: "TRIGGERING DASK FALLBACK MODE"
   ```

4. **Verify Dask is processing**:
   ```bash
   docker compose logs dask-fallback | grep "Processed batch"
   ```

5. **Restart Flink**:
   ```bash
   docker compose start jobmanager taskmanager
   ```

6. **Wait for resume**:
   ```bash
   # Should see: "RESUMING PRIMARY PROCESSING MODE"
   ```

## Future Enhancements

1. **Adaptive Batch Sizing**: Dynamically adjust batch size based on lag
2. **Multi-level Fallback**: Add Pandas-only mode for extreme constraints
3. **Predictive Switching**: Use ML to predict when fallback will be needed
4. **Auto-scaling**: Automatically add Dask workers when needed
5. **Metrics Dashboard**: Dedicated Grafana dashboard for fallback system

## Summary

The Dask fallback system provides:
- ✅ **Automatic** resource-aware switching
- ✅ **Zero data loss** via Kafka checkpoints
- ✅ **No conflicts** with existing Flink pipeline
- ✅ **Lower latency** than complete failure
- ✅ **Easy configuration** via YAML
- ✅ **Production-ready** with proper error handling

It's designed to be a **safety net** that activates only when needed, ensuring continuous stress monitoring even under resource constraints.
