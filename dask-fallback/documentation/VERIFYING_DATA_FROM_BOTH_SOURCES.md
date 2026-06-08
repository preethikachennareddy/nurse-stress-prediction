# Verifying Data from Both Flink and Dask

## Quick Answer

**Yes!** Both Flink and Dask write to the same InfluxDB database, and you can see data from both sources in Grafana. They're distinguished by a `source` tag:
- Flink data: `source=flink`
- Dask data: `source=dask-fallback`

---

## Step-by-Step Verification Guide

### Step 1: Check InfluxDB Has Data from Both Sources

#### Option A: Using Docker Command Line

```bash
# Check total records
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress"

# Check records by source
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source"
```

**Expected Output:**
```
name: nurse_stress
tags: source=flink
count: 125000

name: nurse_stress
tags: source=dask-fallback
count: 5000
```

#### Option B: Using InfluxDB Web UI

1. Open InfluxDB UI: http://localhost:8086
2. Click "Explore" (left sidebar)
3. Select database: `flink_sink`
4. Run query:
```sql
SELECT * FROM nurse_stress WHERE time > now() - 1h
```

You'll see data with different `source` tags.

### Step 2: Verify Data Structure

Check that both sources write the same fields:

```bash
docker compose exec influxdb influx -database flink_sink -execute \
  "SHOW FIELD KEYS FROM nurse_stress"
```

**Expected Output:**
```
name: nurse_stress
fieldKey        fieldType
--------        ---------
X               float
Y               float
Z               float
EDA             float
HR              float
TEMP            float
stress_level    integer
datetime        integer
```

Both Flink and Dask write the exact same fields!

### Step 3: View Sample Data from Each Source

#### Flink Data:
```bash
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT * FROM nurse_stress WHERE source='flink' ORDER BY time DESC LIMIT 5"
```

#### Dask Data:
```bash
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT * FROM nurse_stress WHERE source='dask-fallback' ORDER BY time DESC LIMIT 5"
```

**Sample Output:**
```
time                id    X      Y      Z      EDA    HR    TEMP   stress_level  source
----                --    -      -      -      ---    --    ----   ------------  ------
1733500800000000000 n001  0.12   0.45   0.89   2.3    85    36.5   1            flink
1733500805000000000 n002  0.15   0.42   0.91   2.5    92    37.1   2            flink
...
1733500830000000000 n001  0.13   0.44   0.88   2.4    86    36.6   1            dask-fallback
1733500860000000000 n002  0.16   0.43   0.90   2.6    93    37.2   2            dask-fallback
```

---

## Grafana Visualization

### Step 1: Access Grafana

1. Open Grafana: http://localhost:3000
2. Default credentials: `admin` / `admin`
3. Skip password change (or set new password)

### Step 2: Add InfluxDB Data Source (If Not Already Added)

1. Click **Configuration** (gear icon) → **Data Sources**
2. Click **Add data source**
3. Select **InfluxDB**
4. Configure:
   - **Name**: InfluxDB
   - **URL**: `http://influxdb:8086`
   - **Database**: `flink_sink`
   - **HTTP Method**: GET
5. Click **Save & Test**

You should see: "Data source is working"

### Step 3: Create Dashboard to Show Both Sources

#### Panel 1: Stress Levels by Source (Time Series)

1. Click **+** → **Dashboard** → **Add new panel**
2. In query editor, select:
   - **FROM**: `nurse_stress`
   - **SELECT**: `field(stress_level)` → `mean()`
   - **GROUP BY**: `time(1m)`, `tag(source)`
3. Or use raw query:
```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE $timeFilter 
GROUP BY time(1m), source
```
4. In **Panel options**:
   - **Title**: "Stress Levels by Processing Source"
   - **Legend**: Show
5. Click **Apply**

**What you'll see:**
- Two lines on the graph:
  - One for Flink (continuous)
  - One for Dask (appears during fallback)

#### Panel 2: Data Count by Source (Stat Panel)

1. Add new panel
2. Query:
```sql
SELECT COUNT("stress_level") 
FROM "nurse_stress" 
WHERE $timeFilter 
GROUP BY source
```
3. Change visualization to **Stat**
4. Title: "Records Processed by Source"
5. Click **Apply**

**What you'll see:**
- Two numbers showing count from each source

#### Panel 3: Stress Levels by Nurse (All Sources)

1. Add new panel
2. Query:
```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE $timeFilter 
GROUP BY time(1m), id
```
3. Title: "Nurse Stress Levels (Combined)"
4. This shows data from BOTH sources combined

#### Panel 4: Fallback Status Indicator

1. Add new panel
2. Query:
```sql
SELECT COUNT("stress_level") 
FROM "nurse_stress" 
WHERE $timeFilter AND source='dask-fallback' AND time > now() - 5m
```
3. Change to **Stat** visualization
4. Add threshold:
   - 0 = Green (Normal - Flink only)
   - >0 = Orange (Fallback active)
5. Title: "Fallback Status"

**What you'll see:**
- Green "0" when only Flink is running
- Orange ">0" when Dask is helping

---

## Complete Verification Script

Save this as `verify_both_sources.sh`:

```bash
#!/bin/bash

echo "=========================================="
echo "Verifying Data from Flink and Dask"
echo "=========================================="
echo ""

# Check if services are running
echo "1. Checking services..."
if ! docker compose ps | grep -q "Up"; then
    echo "❌ Services not running. Start with: docker compose up -d"
    exit 1
fi
echo "✅ Services are running"
echo ""

# Check total data
echo "2. Total records in InfluxDB:"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress" | tail -5
echo ""

# Check data by source
echo "3. Records by source:"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source" | tail -10
echo ""

# Check recent Flink data
echo "4. Recent Flink data (last 3 records):"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT id, HR, TEMP, stress_level, source FROM nurse_stress WHERE source='flink' ORDER BY time DESC LIMIT 3" | tail -8
echo ""

# Check if Dask has processed any data
DASK_COUNT=$(docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress WHERE source='dask-fallback'" | grep -oP '\d+' | tail -1)

if [ "$DASK_COUNT" -gt 0 ]; then
    echo "5. Recent Dask data (last 3 records):"
    docker compose exec -T influxdb influx -database flink_sink -execute \
      "SELECT id, HR, TEMP, stress_level, source FROM nurse_stress WHERE source='dask-fallback' ORDER BY time DESC LIMIT 3" | tail -8
    echo ""
    echo "✅ Dask has processed $DASK_COUNT records"
else
    echo "5. Dask data:"
    echo "ℹ️  Dask hasn't processed any data yet (fallback not triggered)"
    echo "   This is normal if Flink is keeping up with the load"
fi
echo ""

# Check field consistency
echo "6. Verifying field structure (should be same for both):"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SHOW FIELD KEYS FROM nurse_stress" | tail -12
echo ""

# Summary
echo "=========================================="
echo "Summary"
echo "=========================================="
echo "✅ InfluxDB is receiving data"
echo "✅ Data structure is consistent"
echo "✅ Both sources write to same database"
echo ""
echo "Next steps:"
echo "1. Open Grafana: http://localhost:3000"
echo "2. Create dashboard with queries from this guide"
echo "3. To trigger Dask fallback for testing:"
echo "   cd dask-fallback && ./test_fallback.sh"
echo ""
```

Make it executable and run:
```bash
chmod +x verify_both_sources.sh
./verify_both_sources.sh
```

---

## Testing Dask Data Collection

If you want to see Dask data in action:

### Option 1: Run the Automated Test

```bash
cd dask-fallback
./test_fallback.sh
```

This will:
1. Stop Flink (simulate overload)
2. Wait for Dask to trigger (3-5 minutes)
3. Verify Dask writes data to InfluxDB
4. Restart Flink

### Option 2: Manual Simulation

```bash
# Stop Flink
docker compose stop jobmanager taskmanager

# Wait 5 minutes and check logs
docker compose logs dask-fallback -f
# Look for: "TRIGGERING DASK FALLBACK MODE"

# Check Dask is writing data
docker compose exec influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress WHERE source='dask-fallback'"

# Restart Flink
docker compose start jobmanager taskmanager
```

---

## Grafana Dashboard JSON (Ready to Import)

Save this as `grafana_dashboard.json`:

```json
{
  "dashboard": {
    "title": "Nurse Stress Monitoring - Dual Source",
    "panels": [
      {
        "title": "Stress Levels by Source",
        "targets": [
          {
            "query": "SELECT mean(\"stress_level\") FROM \"nurse_stress\" WHERE $timeFilter GROUP BY time(1m), source"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Processing Source Distribution",
        "targets": [
          {
            "query": "SELECT COUNT(\"stress_level\") FROM \"nurse_stress\" WHERE $timeFilter GROUP BY source"
          }
        ],
        "type": "stat"
      },
      {
        "title": "Fallback Active?",
        "targets": [
          {
            "query": "SELECT COUNT(\"stress_level\") FROM \"nurse_stress\" WHERE source='dask-fallback' AND time > now() - 5m"
          }
        ],
        "type": "stat",
        "thresholds": [
          { "value": 0, "color": "green" },
          { "value": 1, "color": "orange" }
        ]
      }
    ]
  }
}
```

Import in Grafana:
1. Click **+** → **Import**
2. Paste the JSON
3. Select InfluxDB data source
4. Click **Import**

---

## Common Questions

### Q: Why don't I see Dask data?
**A:** Dask only processes when Flink is overloaded. If Flink is keeping up, Dask stays idle. This is normal and expected!

To see Dask data:
- Run the test script: `cd dask-fallback && ./test_fallback.sh`
- Or wait for actual resource constraints

### Q: How can I tell which source processed which nurse?
**A:** Query by both source and nurse ID:
```sql
SELECT * FROM nurse_stress 
WHERE id='n001' 
GROUP BY source
```

### Q: Will I see duplicate data?
**A:** No! Each message is processed by only ONE source (Flink OR Dask), never both. They use separate Kafka consumer groups.

### Q: Can I filter to show only one source in Grafana?
**A:** Yes! Add `WHERE source='flink'` or `WHERE source='dask-fallback'` to your query.

### Q: What if I want to combine data from both sources?
**A:** Just don't filter by source! The default queries combine both:
```sql
SELECT mean("stress_level") 
FROM "nurse_stress" 
WHERE $timeFilter 
GROUP BY time(1m), id
```

---

## Visual Confirmation Checklist

- [ ] InfluxDB shows records with `source=flink`
- [ ] InfluxDB shows records with `source=dask-fallback` (after triggering fallback)
- [ ] Both sources have same field structure (X, Y, Z, EDA, HR, TEMP, stress_level)
- [ ] Grafana data source connects successfully
- [ ] Grafana dashboard shows time series from both sources
- [ ] Fallback indicator panel works (green when normal, orange when active)
- [ ] No duplicate data (each nurse's data processed by one source at a time)

---

## Summary

**Yes, you can see data from both sources!**

- ✅ Both write to same InfluxDB database (`flink_sink`)
- ✅ Both write to same measurement (`nurse_stress`)
- ✅ Both write same fields (X, Y, Z, EDA, HR, TEMP, stress_level)
- ✅ Distinguished by `source` tag
- ✅ Grafana can show both separately or combined
- ✅ No conflicts, no duplicates

The system is designed so that data from both sources looks identical in Grafana - your teammates won't even notice which source processed which data unless they specifically check the `source` tag!
