# How to Use the Dask Fallback System

## TL;DR - Quick Start

```bash
# 1. Start everything
cd project
docker compose up -d

# 2. Watch it work
docker compose logs dask-fallback -f

# 3. That's it! The system monitors and switches automatically.
```

## What You Need to Know

### The System is Automatic

**You don't need to do anything.** The Dask fallback system:
- ✅ Starts automatically when you run `docker compose up`
- ✅ Monitors Flink processing health continuously
- ✅ Switches to Dask when resource constraints detected
- ✅ Returns to Flink when resources stabilize
- ✅ Ensures no data loss during transitions

### When Does Fallback Trigger?

The system switches to Dask when:
1. **Flink processing latency** exceeds 5 minutes, AND
2. This happens for **3 consecutive checks** (60 seconds apart), OR
3. **Flink is down** or unhealthy

### When Does It Resume Primary?

The system returns to Flink-only when:
1. **Processing latency** drops below 2.5 minutes, AND
2. **Flink is healthy** and responding

## What to Expect

### Normal Operation

```bash
docker compose logs dask-fallback --tail 20
```

You'll see:
```
Starting latency monitor...
Monitoring Flink/Spark processing health...
Current processing latency: 2.34s
Flink is healthy
```

**This means**: Everything is normal, Flink is handling the load.

### Fallback Triggered

```
Latency violation 1/3: 312.45s > 300s
Latency violation 2/3: 325.67s > 300s
Latency violation 3/3: 318.23s > 300s
TRIGGERING DASK FALLBACK MODE
Starting Dask fallback processor...
Collected 1000 messages in 28.45s
Processed batch in 12.34s
Wrote 15 points to InfluxDB
```

**This means**: Flink is struggling, Dask is helping process the backlog.

### Back to Normal

```
Primary processing path is healthy
RESUMING PRIMARY PROCESSING MODE
Stopping Dask fallback processor...
Returned to primary processing path
```

**This means**: Flink caught up, Dask stopped, back to normal.

## Common Tasks

### Check System Status

```bash
# Are all services running?
docker compose ps

# What's the current mode?
docker compose logs dask-fallback --tail 5

# Is data being processed?
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress"
```

### View Processing Sources

```bash
# How much data from each source?
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source"
```

Expected output:
```
name: nurse_stress
tags: source=flink
count: 125000

name: nurse_stress
tags: source=dask-fallback
count: 5000
```

### Test the Fallback

```bash
cd dask-fallback
./test_fallback.sh
```

This will:
1. Stop Flink (simulate overload)
2. Wait for Dask to trigger
3. Verify Dask processes data
4. Restart Flink
5. Verify return to normal

Takes about 8-10 minutes.

### Adjust Sensitivity

If fallback triggers too often or not often enough:

```bash
# Edit configuration
vim dask-fallback/config.yaml

# Change these values:
monitoring:
  latency_threshold_seconds: 300  # Increase to trigger less often
  consecutive_violations: 3        # Increase to trigger less often

# Restart to apply
docker compose restart dask-fallback
```

### Disable Fallback

If you want to disable the automatic fallback:

```bash
# Stop Dask fallback
docker compose stop dask-fallback

# Your system continues with Flink only
```

To re-enable:
```bash
docker compose start dask-fallback
```

## Understanding the Logs

### Key Log Messages

| Message | Meaning | Action Needed |
|---------|---------|---------------|
| "Starting latency monitor" | System started | None - normal |
| "Current processing latency: X.XXs" | Regular health check | None - normal |
| "Flink is healthy" | Flink working well | None - normal |
| "Latency violation X/3" | Potential issue detected | None - monitoring |
| "TRIGGERING DASK FALLBACK" | Switching to fallback | None - automatic |
| "Collected X messages" | Dask processing | None - normal |
| "Processed batch in X.XXs" | Batch completed | None - normal |
| "RESUMING PRIMARY" | Returning to normal | None - automatic |
| "Error calling ML service" | ML service issue | Check ML service |
| "Error writing to InfluxDB" | Database issue | Check InfluxDB |

### Viewing Logs

```bash
# Live logs (follow mode)
docker compose logs dask-fallback -f

# Last 100 lines
docker compose logs dask-fallback --tail 100

# Search for errors
docker compose logs dask-fallback | grep -i error

# Search for mode changes
docker compose logs dask-fallback | grep "TRIGGERING\|RESUMING"
```

## Grafana Integration

### View Both Sources

Update your Grafana queries to show both Flink and Dask data:

**Before:**
```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE source='flink'
GROUP BY time(1m), id
```

**After:**
```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE source='flink' OR source='dask-fallback'
GROUP BY time(1m), id, source
```

### Add Fallback Indicator

Create a new panel to show when fallback is active:

```sql
SELECT COUNT(*) 
FROM "nurse_stress" 
WHERE source='dask-fallback' AND time > now() - 5m
```

- If count > 0: Fallback is active
- If count = 0: Normal mode

## Troubleshooting

### Problem: Dask container keeps restarting

**Check:**
```bash
docker compose logs dask-fallback
```

**Common causes:**
1. Kafka not ready → Wait 30 seconds, restart: `docker compose restart dask-fallback`
2. ML service down → Check: `docker compose ps stress-prediction-service`
3. InfluxDB not ready → Check: `docker compose ps influxdb`

### Problem: Fallback not triggering when Flink is slow

**Check:**
```bash
# Is monitor running?
docker compose logs dask-fallback | grep "Starting latency monitor"

# What's the current latency?
docker compose logs dask-fallback | grep "Current processing latency"

# Is InfluxDB accessible?
docker compose exec dask-fallback curl http://influxdb:8086/ping
```

**Solution:**
Lower the threshold temporarily:
```bash
vim dask-fallback/config.yaml
# Set latency_threshold_seconds: 60
docker compose restart dask-fallback
```

### Problem: Fallback triggers too often

**Solution:**
Increase thresholds:
```bash
vim dask-fallback/config.yaml
# Set latency_threshold_seconds: 600
# Set consecutive_violations: 5
docker compose restart dask-fallback
```

### Problem: High memory usage

**Solution:**
Reduce batch size:
```bash
vim dask-fallback/config.yaml
# Set batch_size: 500
# Set memory_limit: "1GB"
docker compose restart dask-fallback
```

## Configuration Reference

### Key Settings

```yaml
# How sensitive is the trigger?
monitoring:
  latency_threshold_seconds: 300  # Lower = more sensitive
  consecutive_violations: 3        # Lower = more sensitive
  check_interval_seconds: 60       # How often to check

# How does Dask process?
processing:
  batch_size: 1000                 # Larger = less frequent
  batch_timeout_seconds: 30        # Max wait time

# Dask resources
dask:
  n_workers: 2                     # More = faster processing
  memory_limit: "2GB"              # Per worker
```

### Tuning Examples

**More sensitive (triggers faster):**
```yaml
monitoring:
  latency_threshold_seconds: 180
  consecutive_violations: 2
```

**Less sensitive (triggers slower):**
```yaml
monitoring:
  latency_threshold_seconds: 600
  consecutive_violations: 5
```

**Lower latency (smaller batches):**
```yaml
processing:
  batch_size: 500
  batch_timeout_seconds: 15
```

**Higher throughput (larger batches):**
```yaml
processing:
  batch_size: 2000
  batch_timeout_seconds: 60
```

## Best Practices

### 1. Monitor First
Let the system run for 24 hours before making changes. Observe:
- How often does fallback trigger?
- What's the typical latency?
- Are there patterns (time of day, etc.)?

### 2. Tune Gradually
Don't change multiple settings at once:
- Change one parameter
- Restart and observe for a few hours
- Adjust again if needed

### 3. Check Regularly
Weekly checks:
```bash
# How many times did fallback trigger?
docker compose logs dask-fallback | grep "TRIGGERING" | wc -l

# What's the data distribution?
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source"
```

### 4. Keep Logs
Save logs periodically:
```bash
docker compose logs dask-fallback > dask_logs_$(date +%Y%m%d).txt
```

### 5. Test Periodically
Run the test script monthly:
```bash
cd dask-fallback
./test_fallback.sh
```

## FAQ

**Q: Does this affect my existing Flink pipeline?**
A: No. Dask uses a separate Kafka consumer group and only helps when Flink is overloaded.

**Q: Will I lose data during switching?**
A: No. Kafka checkpoints ensure no data loss during transitions.

**Q: Can I run Dask all the time instead of Flink?**
A: Yes, but not recommended. Flink has lower latency (5s vs 30s). Use Dask as a fallback only.

**Q: How do I know which source processed which data?**
A: Check the `source` tag in InfluxDB: `flink` or `dask-fallback`.

**Q: Can I manually trigger fallback?**
A: Not directly, but you can lower the threshold to 0 to force it:
```yaml
latency_threshold_seconds: 0
```

**Q: What happens if both Flink and Dask fail?**
A: Data accumulates in Kafka. When either recovers, it processes from the checkpoint.

**Q: Does this work with Spark instead of Flink?**
A: Yes! The monitor checks InfluxDB latency, not Flink specifically. Works with any processor.

**Q: Can I use this in production?**
A: Yes. It's designed for production with proper error handling, logging, and graceful shutdown.

## Getting Help

### Documentation
- 📖 [Integration Guide](DASK_FALLBACK_INTEGRATION.md) - How it works
- 📚 [Full Documentation](dask-fallback/README.md) - Technical details
- 🏗️ [Architecture](dask-fallback/ARCHITECTURE.md) - System design
- 🚀 [Quick Start](dask-fallback/QUICK_START.md) - Commands reference
- 🚢 [Deployment Guide](DEPLOYMENT_GUIDE.md) - Production deployment

### Commands
```bash
# Check status
docker compose ps

# View logs
docker compose logs dask-fallback -f

# Test system
cd dask-fallback && ./test_fallback.sh

# Restart service
docker compose restart dask-fallback
```

### Logs to Collect
If you need help, collect these:
```bash
docker compose logs dask-fallback > dask_logs.txt
docker compose logs kafka > kafka_logs.txt
docker compose logs influxdb > influxdb_logs.txt
docker compose ps > status.txt
```

## Summary

The Dask fallback system is a **set-it-and-forget-it** safety net:

1. **Start it**: `docker compose up -d`
2. **Monitor it**: `docker compose logs dask-fallback -f`
3. **Trust it**: It handles everything automatically

No manual intervention needed. It just works.

---

**Need more details?** Check the [full documentation](dask-fallback/README.md).

**Want to test it?** Run `cd dask-fallback && ./test_fallback.sh`.

**Have issues?** Check the [troubleshooting section](#troubleshooting) above.
