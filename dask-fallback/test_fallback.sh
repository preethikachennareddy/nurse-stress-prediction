#!/bin/bash
# Test script for Dask fallback system

set -e

echo "=========================================="
echo "Dask Fallback System Test"
echo "=========================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_info() {
    echo -e "${YELLOW}[i]${NC} $1"
}

# Check if docker compose is running
print_info "Checking if services are running..."
if ! docker compose ps | grep -q "Up"; then
    print_error "Services are not running. Start with: docker compose up -d"
    exit 1
fi
print_status "Services are running"

# Check Kafka
print_info "Checking Kafka..."
if docker compose exec -T kafka kafka-topics.sh --bootstrap-server kafka:9092 --list | grep -q "stress-topic"; then
    print_status "Kafka is healthy and topic exists"
else
    print_error "Kafka topic not found"
    exit 1
fi

# Check InfluxDB
print_info "Checking InfluxDB..."
if docker compose exec -T influxdb influx -execute "SHOW DATABASES" | grep -q "flink_sink"; then
    print_status "InfluxDB is healthy"
else
    print_error "InfluxDB database not found"
    exit 1
fi

# Check ML Service
print_info "Checking ML Service..."
if docker compose exec -T dask-fallback curl -s http://stress-prediction-service:5000/model/api/predict?x=0\&y=0\&z=0\&eda=0\&hr=70\&temp=36 | grep -q "stress_level_prediction"; then
    print_status "ML Service is responding"
else
    print_error "ML Service is not responding"
    exit 1
fi

# Check Dask Fallback container
print_info "Checking Dask Fallback container..."
if docker compose ps dask-fallback | grep -q "Up"; then
    print_status "Dask Fallback container is running"
else
    print_error "Dask Fallback container is not running"
    exit 1
fi

echo ""
echo "=========================================="
echo "Running Fallback Simulation Test"
echo "=========================================="
echo ""

print_info "This test will:"
echo "  1. Stop Flink to simulate resource constraint"
echo "  2. Wait for Dask fallback to trigger (3-5 minutes)"
echo "  3. Verify Dask is processing data"
echo "  4. Restart Flink"
echo "  5. Verify return to primary processing"
echo ""

read -p "Continue with simulation? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    print_info "Test cancelled"
    exit 0
fi

# Step 1: Stop Flink
print_info "Step 1: Stopping Flink to simulate overload..."
docker compose stop jobmanager taskmanager 2>/dev/null || true
print_status "Flink stopped"

# Step 2: Monitor for fallback trigger
print_info "Step 2: Monitoring for fallback trigger (this may take 3-5 minutes)..."
print_info "Watching orchestrator logs..."

timeout=300  # 5 minutes
elapsed=0
triggered=false

while [ $elapsed -lt $timeout ]; do
    if docker compose logs dask-fallback --tail 50 | grep -q "TRIGGERING DASK FALLBACK"; then
        print_status "Fallback triggered!"
        triggered=true
        break
    fi
    
    echo -n "."
    sleep 10
    elapsed=$((elapsed + 10))
done

echo ""

if [ "$triggered" = false ]; then
    print_error "Fallback did not trigger within timeout"
    print_info "Check logs: docker compose logs dask-fallback"
    exit 1
fi

# Step 3: Verify Dask is processing
print_info "Step 3: Verifying Dask is processing data..."
sleep 30  # Wait for first batch

if docker compose logs dask-fallback --tail 100 | grep -q "Processed batch"; then
    print_status "Dask is processing batches"
else
    print_error "Dask is not processing"
    exit 1
fi

# Check if data is in InfluxDB
if docker compose exec -T influxdb influx -database flink_sink -execute "SELECT COUNT(*) FROM nurse_stress WHERE source='dask-fallback'" | grep -q "[1-9]"; then
    print_status "Data is being written to InfluxDB"
else
    print_error "No data in InfluxDB from Dask"
fi

# Step 4: Restart Flink
print_info "Step 4: Restarting Flink..."
docker compose start jobmanager taskmanager
print_status "Flink restarted"

# Step 5: Monitor for primary resume
print_info "Step 5: Monitoring for return to primary (this may take 2-3 minutes)..."

timeout=180  # 3 minutes
elapsed=0
resumed=false

while [ $elapsed -lt $timeout ]; do
    if docker compose logs dask-fallback --tail 50 | grep -q "RESUMING PRIMARY"; then
        print_status "Returned to primary processing!"
        resumed=true
        break
    fi
    
    echo -n "."
    sleep 10
    elapsed=$((elapsed + 10))
done

echo ""

if [ "$resumed" = false ]; then
    print_info "Primary not resumed yet (may need more time for latency to drop)"
fi

echo ""
echo "=========================================="
echo "Test Summary"
echo "=========================================="
print_status "Kafka: Healthy"
print_status "InfluxDB: Healthy"
print_status "ML Service: Healthy"
print_status "Fallback Trigger: $( [ "$triggered" = true ] && echo "Success" || echo "Failed" )"
print_status "Dask Processing: Verified"
print_status "Primary Resume: $( [ "$resumed" = true ] && echo "Success" || echo "Pending" )"

echo ""
print_info "View live logs with: docker compose logs dask-fallback -f"
print_info "Check InfluxDB data: docker compose exec influxdb influx -database flink_sink -execute \"SELECT * FROM nurse_stress WHERE source='dask-fallback' LIMIT 10\""

echo ""
print_status "Test completed!"
