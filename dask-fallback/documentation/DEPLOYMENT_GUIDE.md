# Deployment Guide - Dask Fallback System

## Pre-Deployment Checklist

### System Requirements
- [ ] Docker Desktop installed (v20.10+)
- [ ] Docker Compose v2 installed
- [ ] 8 GB RAM available (minimum)
- [ ] 10 GB disk space available
- [ ] Ports available: 2181, 8000, 8080, 8081, 8086, 9092, 3000

### Verification Commands
```bash
# Check Docker
docker --version
# Expected: Docker version 20.10.0 or higher

# Check Docker Compose
docker compose version
# Expected: Docker Compose version v2.0.0 or higher

# Check available memory
docker info | grep "Total Memory"
# Expected: 8 GB or more

# Check ports
netstat -an | grep -E "2181|8000|8080|8081|8086|9092|3000"
# Expected: No output (ports are free)
```

## Deployment Steps

### Step 1: Clone and Navigate
```bash
# If not already cloned
git clone https://github.com/peppermintflowers/nurse_stress_prediction.git
cd nurse_stress_prediction

# Verify you're in the right directory
ls -la
# Should see: docker-compose.yml, data/, dask-fallback/, etc.
```

### Step 2: Review Configuration
```bash
# Check Dask fallback configuration
cat dask-fallback/config.yaml

# Adjust if needed (optional)
vim dask-fallback/config.yaml
```

**Key settings to review:**
- `latency_threshold_seconds`: How long before triggering fallback (default: 300s)
- `batch_size`: Messages per batch (default: 1000)
- `consecutive_violations`: How many checks before switching (default: 3)

### Step 3: Build All Services
```bash
# Build all containers (including Dask fallback)
docker compose build

# This will take 5-10 minutes on first run
# Watch for any build errors
```

**Expected output:**
```
[+] Building 45.2s (23/23) FINISHED
 => [dask-fallback internal] load build definition
 => [dask-fallback] transferring dockerfile
 ...
 => [dask-fallback] exporting to image
```

### Step 4: Start Services
```bash
# Start all services in detached mode
docker compose up -d

# Wait 30 seconds for services to initialize
sleep 30

# Verify all services are running
docker compose ps
```

**Expected output:**
```
NAME                    STATUS
csv-producer            Up
dask-fallback           Up
grafana                 Up
influxdb                Up
jobmanager              Up
kafka                   Up
kafka-ui                Up
stress-prediction-service Up
taskmanager             Up
zookeeper               Up
```

### Step 5: Verify Service Health

#### Check Kafka
```bash
docker compose exec kafka kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```
**Expected**: Should show `stress-topic`

#### Check InfluxDB
```bash
docker compose exec influxdb influx -execute "SHOW DATABASES"
```
**Expected**: Should show `flink_sink`

#### Check ML Service
```bash
curl "http://localhost:8000/model/api/predict?x=0&y=0&z=0&eda=0&hr=70&temp=36"
```
**Expected**: `{"stress_level_prediction":"1"}`

#### Check Flink
```bash
curl http://localhost:8081/overview
```
**Expected**: JSON response with Flink status

#### Check Dask Fallback
```bash
docker compose logs dask-fallback --tail 20
```
**Expected**: Should see "Starting latency monitor..."

### Step 6: Monitor Initial Operation
```bash
# Watch Dask fallback logs
docker compose logs dask-fallback -f

# In another terminal, watch producer
docker compose logs csv-producer -f

# In another terminal, watch Flink
docker compose logs taskmanager -f
```

**What to look for:**
- Dask: "Current processing latency: X.XXs"
- Producer: "Final batch of X messages sent"
- Flink: No errors

### Step 7: Verify Data Flow

#### Check Kafka Messages
```bash
# Open Kafka UI
open http://localhost:8080
# Navigate to Topics → stress-topic → Messages
```

#### Check InfluxDB Data
```bash
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress"
```
**Expected**: Count should be increasing

#### Check Grafana
```bash
# Open Grafana
open http://localhost:3000
# Default credentials: admin/admin
```

### Step 8: Test Fallback System
```bash
cd dask-fallback
./test_fallback.sh
```

This will:
1. Verify all services are healthy
2. Simulate resource constraint
3. Confirm fallback triggers
4. Verify Dask processes data
5. Confirm return to primary

**Expected duration**: 8-10 minutes

## Post-Deployment Verification

### Health Check Script
Create `health_check.sh`:
```bash
#!/bin/bash

echo "=== Health Check ==="

# Check containers
echo "1. Checking containers..."
if [ $(docker compose ps | grep "Up" | wc -l) -eq 10 ]; then
    echo "✓ All 10 containers running"
else
    echo "✗ Some containers are down"
    docker compose ps
fi

# Check Kafka
echo "2. Checking Kafka..."
if docker compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q "stress-topic"; then
    echo "✓ Kafka topic exists"
else
    echo "✗ Kafka topic missing"
fi

# Check InfluxDB
echo "3. Checking InfluxDB..."
COUNT=$(docker compose exec -T influxdb influx -database flink_sink -execute "SELECT COUNT(*) FROM nurse_stress" | tail -1 | awk '{print $2}')
if [ "$COUNT" -gt 0 ]; then
    echo "✓ InfluxDB has $COUNT records"
else
    echo "✗ No data in InfluxDB"
fi

# Check ML Service
echo "4. Checking ML Service..."
if curl -s "http://localhost:8000/model/api/predict?x=0&y=0&z=0&eda=0&hr=70&temp=36" | grep -q "stress_level_prediction"; then
    echo "✓ ML Service responding"
else
    echo "✗ ML Service not responding"
fi

# Check Dask Fallback
echo "5. Checking Dask Fallback..."
if docker compose logs dask-fallback --tail 10 | grep -q "latency"; then
    echo "✓ Dask Fallback monitoring"
else
    echo "✗ Dask Fallback not monitoring"
fi

echo "=== Health Check Complete ==="
```

```bash
chmod +x health_check.sh
./health_check.sh
```

### Monitoring Dashboard

#### Create Grafana Dashboard
1. Open Grafana: http://localhost:3000
2. Add InfluxDB data source:
   - URL: http://influxdb:8086
   - Database: flink_sink
3. Create dashboard with panels:
   - **Stress Levels by Nurse**: `SELECT mean("stress_level") FROM "nurse_stress" GROUP BY time(1m), id`
   - **Processing Source**: `SELECT COUNT(*) FROM "nurse_stress" GROUP BY time(1m), source`
   - **Fallback Status**: `SELECT COUNT(*) FROM "nurse_stress" WHERE source='dask-fallback' AND time > now() - 5m`

#### Set Up Alerts (Optional)
Create alert when fallback is active:
```sql
SELECT COUNT(*) 
FROM "nurse_stress" 
WHERE source='dask-fallback' AND time > now() - 5m
```
Alert if count > 0

## Configuration Tuning

### For Production Environment

#### Increase Sensitivity (Faster Fallback)
```yaml
# dask-fallback/config.yaml
monitoring:
  latency_threshold_seconds: 180  # 3 minutes instead of 5
  consecutive_violations: 2        # 2 checks instead of 3
```

#### Decrease Sensitivity (Slower Fallback)
```yaml
monitoring:
  latency_threshold_seconds: 600  # 10 minutes instead of 5
  consecutive_violations: 5        # 5 checks instead of 3
```

#### Optimize for Low Latency
```yaml
processing:
  batch_size: 500                  # Smaller batches
  batch_timeout_seconds: 15        # Shorter timeout
```

#### Optimize for High Throughput
```yaml
processing:
  batch_size: 2000                 # Larger batches
  batch_timeout_seconds: 60        # Longer timeout
```

### Apply Configuration Changes
```bash
# Edit config
vim dask-fallback/config.yaml

# Restart Dask fallback
docker compose restart dask-fallback

# Verify changes
docker compose logs dask-fallback --tail 20
```

## Troubleshooting

### Issue: Dask Container Won't Start

**Symptoms:**
```bash
docker compose ps
# dask-fallback: Restarting
```

**Solutions:**
```bash
# Check logs
docker compose logs dask-fallback

# Common causes:
# 1. Kafka not ready
docker compose restart dask-fallback

# 2. Missing Avro schema
docker compose exec dask-fallback ls -la /data/nurse_sensor_event.avsc

# 3. ML service not reachable
docker compose exec dask-fallback curl http://stress-prediction-service:5000/model/api/predict?x=0\&y=0\&z=0\&eda=0\&hr=70\&temp=36
```

### Issue: Fallback Not Triggering

**Symptoms:**
- Flink is slow but Dask doesn't start

**Solutions:**
```bash
# Check monitor is running
docker compose logs dask-fallback | grep "Starting latency monitor"

# Check if InfluxDB has data
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT * FROM nurse_stress ORDER BY time DESC LIMIT 1"

# Check Flink health
curl http://localhost:8081/overview

# Lower threshold temporarily
vim dask-fallback/config.yaml
# Set latency_threshold_seconds: 60
docker compose restart dask-fallback
```

### Issue: High Memory Usage

**Symptoms:**
- System running out of memory
- Containers being killed

**Solutions:**
```bash
# Check memory usage
docker stats

# Reduce Dask batch size
vim dask-fallback/config.yaml
# Set batch_size: 500
# Set memory_limit: "1GB"

# Add memory limits to docker-compose.yml
vim docker-compose.yml
# Add under dask-fallback:
#   deploy:
#     resources:
#       limits:
#         memory: 2G

# Restart
docker compose restart dask-fallback
```

### Issue: Data Not in InfluxDB

**Symptoms:**
- Dask logs show processing but no data in InfluxDB

**Solutions:**
```bash
# Check InfluxDB connection
docker compose exec dask-fallback python -c \
  "from influxdb import InfluxDBClient; c = InfluxDBClient('influxdb', 8086); print(c.ping())"

# Check database exists
docker compose exec influxdb influx -execute "SHOW DATABASES"

# Check for errors
docker compose logs dask-fallback | grep -i error

# Verify ML service is working
docker compose logs stress-prediction-service
```

## Maintenance

### Daily Checks
```bash
# Check all services are running
docker compose ps

# Check disk usage
docker system df

# Check logs for errors
docker compose logs --tail 100 | grep -i error
```

### Weekly Checks
```bash
# Check data volume
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress"

# Check Kafka consumer lag
docker compose exec kafka kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --all-groups

# Review fallback triggers
docker compose logs dask-fallback | grep "TRIGGERING\|RESUMING"
```

### Monthly Maintenance
```bash
# Clean up old logs
docker compose logs --tail 1000 > logs_backup.txt
docker compose restart dask-fallback

# Update images
docker compose pull
docker compose up -d

# Backup InfluxDB
docker compose exec influxdb influxd backup -database flink_sink /tmp/backup
docker compose cp influxdb:/tmp/backup ./influxdb_backup_$(date +%Y%m%d)
```

## Scaling

### Horizontal Scaling (Multiple Dask Workers)

Add to `docker-compose.yml`:
```yaml
dask-scheduler:
  image: daskdev/dask:latest
  command: ["dask-scheduler"]
  ports:
    - "8786:8786"
    - "8787:8787"
  networks:
    - app-net

dask-worker-1:
  image: daskdev/dask:latest
  command: ["dask-worker", "dask-scheduler:8786"]
  depends_on:
    - dask-scheduler
  networks:
    - app-net

dask-worker-2:
  image: daskdev/dask:latest
  command: ["dask-worker", "dask-scheduler:8786"]
  depends_on:
    - dask-scheduler
  networks:
    - app-net
```

Update `dask-fallback/config.yaml`:
```yaml
dask:
  scheduler_address: "dask-scheduler:8786"
```

### Vertical Scaling (More Resources)

Edit `docker-compose.yml`:
```yaml
dask-fallback:
  deploy:
    resources:
      limits:
        cpus: '2.0'
        memory: 4G
      reservations:
        cpus: '1.0'
        memory: 2G
```

## Rollback Procedure

### If Issues Occur

#### Option 1: Disable Dask Fallback
```bash
# Stop Dask only
docker compose stop dask-fallback

# System continues with Flink only
```

#### Option 2: Complete Rollback
```bash
# Stop all services
docker compose down

# Remove Dask service from docker-compose.yml
vim docker-compose.yml
# Delete the dask-fallback section

# Restart without Dask
docker compose up -d
```

#### Option 3: Restore Previous Version
```bash
# Stop all
docker compose down

# Checkout previous commit
git log --oneline
git checkout <previous-commit>

# Restart
docker compose up -d
```

## Success Criteria

Your deployment is successful when:

- ✅ All 10 containers are running
- ✅ Kafka topic `stress-topic` exists and has messages
- ✅ InfluxDB has data in `nurse_stress` measurement
- ✅ ML service responds to prediction requests
- ✅ Flink is processing data (check Flink UI at :8081)
- ✅ Dask fallback is monitoring (check logs)
- ✅ Grafana shows stress level visualizations
- ✅ Test script passes all checks

## Support

### Logs to Collect for Issues
```bash
# Collect all logs
docker compose logs > full_logs.txt

# Collect specific service logs
docker compose logs dask-fallback > dask_logs.txt
docker compose logs kafka > kafka_logs.txt
docker compose logs influxdb > influxdb_logs.txt

# System info
docker info > docker_info.txt
docker compose ps > containers_status.txt
```

### Useful Commands Reference
```bash
# View logs
docker compose logs -f [service]

# Restart service
docker compose restart [service]

# Check resource usage
docker stats

# Execute command in container
docker compose exec [service] [command]

# View container details
docker compose ps
docker inspect [container]

# Clean up
docker compose down
docker system prune -a
```

## Next Steps

After successful deployment:

1. **Monitor for 24 hours**: Watch logs and metrics
2. **Tune thresholds**: Adjust based on actual latency patterns
3. **Set up alerts**: Configure Grafana alerts for fallback events
4. **Document patterns**: Note when fallback triggers and why
5. **Optimize**: Adjust batch sizes and timeouts as needed

---

**Deployment Complete!** Your Dask fallback system is now running and will automatically handle resource constraints.
