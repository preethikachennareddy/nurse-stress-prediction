# Dask Fallback - Quick Start

## 🚀 Start Everything

```bash
cd project
docker compose up -d
```

## 📊 Monitor Status

```bash
# Watch orchestrator
docker compose logs dask-fallback -f

# Check all services
docker compose ps
```

## 🧪 Test Fallback

```bash
cd dask-fallback
./test_fallback.sh
```

## ⚙️ Configure

Edit `dask-fallback/config.yaml`:

```yaml
monitoring:
  latency_threshold_seconds: 300  # Trigger threshold
  consecutive_violations: 3        # How many checks
  
processing:
  batch_size: 1000                 # Messages per batch
  batch_timeout_seconds: 30        # Max wait time
```

## 🔍 Check Data

```bash
# View Dask-processed data
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT * FROM nurse_stress WHERE source='dask-fallback' LIMIT 10"

# Count records by source
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source"
```

## 🛠️ Troubleshoot

```bash
# Check logs
docker compose logs dask-fallback --tail 100

# Restart service
docker compose restart dask-fallback

# Check Kafka consumer groups
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --list
```

## 🎯 Key Commands

| Action | Command |
|--------|---------|
| Start all | `docker compose up -d` |
| Stop Dask | `docker compose stop dask-fallback` |
| View logs | `docker compose logs dask-fallback -f` |
| Restart | `docker compose restart dask-fallback` |
| Test | `cd dask-fallback && ./test_fallback.sh` |

## 📈 What to Expect

### Normal Operation
```
Starting latency monitor...
Current processing latency: 2.34s
Flink is healthy
```

### Fallback Triggered
```
Latency violation 3/3: 318.23s > 300s
TRIGGERING DASK FALLBACK MODE
Dask is now processing alongside primary path
Collected 1000 messages in 28.45s
Processed batch in 12.34s
Wrote 15 points to InfluxDB
```

### Back to Normal
```
Primary processing path is healthy
RESUMING PRIMARY PROCESSING MODE
Stopping Dask fallback processor...
```

## 🔧 Common Adjustments

### More Sensitive (triggers faster)
```yaml
monitoring:
  latency_threshold_seconds: 180
  consecutive_violations: 2
```

### Less Sensitive (triggers slower)
```yaml
monitoring:
  latency_threshold_seconds: 600
  consecutive_violations: 5
```

### Smaller Batches (lower latency)
```yaml
processing:
  batch_size: 500
  batch_timeout_seconds: 15
```

### Larger Batches (higher throughput)
```yaml
processing:
  batch_size: 2000
  batch_timeout_seconds: 60
```

## 📚 Documentation

- Full docs: `dask-fallback/README.md`
- Integration guide: `DASK_FALLBACK_INTEGRATION.md`
- Configuration: `dask-fallback/config.yaml`

## ✅ Health Checks

```bash
# All services healthy?
docker compose ps | grep "Up"

# Kafka topic exists?
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 --list | grep stress-topic

# ML service responding?
curl "http://localhost:8000/model/api/predict?x=0&y=0&z=0&eda=0&hr=70&temp=36"

# InfluxDB has data?
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress"
```

## 🚨 Emergency Stop

```bash
# Stop Dask only
docker compose stop dask-fallback

# Stop everything
docker compose down

# Nuclear option (removes all data)
docker compose down -v
```

## 💡 Tips

1. **Monitor first**: Let it run for a few hours to see normal behavior
2. **Adjust thresholds**: Tune based on your actual latency patterns
3. **Test regularly**: Run `test_fallback.sh` to ensure it works
4. **Check Grafana**: Update queries to show both Flink and Dask data
5. **Watch resources**: Monitor CPU/memory usage during fallback

## 🎓 Understanding the Flow

```
Normal: Kafka → Flink (5s) → ML → InfluxDB
        Dask: Monitoring only

Overload: Kafka → Flink (slow) → ML → InfluxDB
               ↘ Dask (30s) → ML → InfluxDB

Resume: Kafka → Flink (5s) → ML → InfluxDB
        Dask: Stopped
```

## 📞 Getting Help

1. Check logs: `docker compose logs dask-fallback`
2. Run test: `./test_fallback.sh`
3. Review config: `cat config.yaml`
4. Read docs: `README.md`

---

**That's it!** The system monitors itself and handles fallback automatically. You just need to start it and let it run.
