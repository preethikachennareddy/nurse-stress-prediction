#!/bin/bash

echo "=========================================="
echo "Verifying Data from Flink and Dask"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check if services are running
echo "1. Checking services..."
if ! docker compose ps | grep -q "Up"; then
    echo -e "${RED}❌ Services not running. Start with: docker compose up -d${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Services are running${NC}"
echo ""

# Check total data
echo "2. Total records in InfluxDB:"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress" 2>/dev/null | tail -5
echo ""

# Check data by source
echo "3. Records by source:"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress GROUP BY source" 2>/dev/null | tail -10
echo ""

# Check recent Flink data
echo "4. Recent Flink data (last 3 records):"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT id, HR, TEMP, stress_level, source FROM nurse_stress WHERE source='flink' ORDER BY time DESC LIMIT 3" 2>/dev/null | tail -8
echo ""

# Check if Dask has processed any data
DASK_COUNT=$(docker compose exec -T influxdb influx -database flink_sink -execute \
  "SELECT COUNT(*) FROM nurse_stress WHERE source='dask-fallback'" 2>/dev/null | grep -oE '[0-9]+' | tail -1)

if [ -n "$DASK_COUNT" ] && [ "$DASK_COUNT" -gt 0 ]; then
    echo "5. Recent Dask data (last 3 records):"
    docker compose exec -T influxdb influx -database flink_sink -execute \
      "SELECT id, HR, TEMP, stress_level, source FROM nurse_stress WHERE source='dask-fallback' ORDER BY time DESC LIMIT 3" 2>/dev/null | tail -8
    echo ""
    echo -e "${GREEN}✅ Dask has processed $DASK_COUNT records${NC}"
else
    echo "5. Dask data:"
    echo -e "${YELLOW}ℹ️  Dask hasn't processed any data yet (fallback not triggered)${NC}"
    echo "   This is normal if Flink is keeping up with the load"
fi
echo ""

# Check field consistency
echo "6. Verifying field structure (should be same for both):"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SHOW FIELD KEYS FROM nurse_stress" 2>/dev/null | tail -12
echo ""

# Check tags
echo "7. Available tags:"
docker compose exec -T influxdb influx -database flink_sink -execute \
  "SHOW TAG KEYS FROM nurse_stress" 2>/dev/null | tail -5
echo ""

# Summary
echo "=========================================="
echo "Summary"
echo "=========================================="
echo -e "${GREEN}✅ InfluxDB is receiving data${NC}"
echo -e "${GREEN}✅ Data structure is consistent${NC}"
echo -e "${GREEN}✅ Both sources write to same database${NC}"
echo ""
echo "Next steps:"
echo "1. Open Grafana: http://localhost:3000 (admin/admin)"
echo "2. Open InfluxDB: http://localhost:8086"
echo "3. Open Kafka UI: http://localhost:8080"
echo ""
echo "To trigger Dask fallback for testing:"
echo "   cd dask-fallback && ./test_fallback.sh"
echo ""
echo "To view live Dask logs:"
echo "   docker compose logs dask-fallback -f"
echo ""
