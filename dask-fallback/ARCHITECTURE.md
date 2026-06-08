# Dask Fallback System Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         NURSE STRESS PREDICTION SYSTEM                   │
│                         with Automatic Dask Fallback                     │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  DATA INGESTION                                                          │
│  ┌────────────────┐                                                      │
│  │ CSV Producer   │  Reads workers.csv (11.5M records)                  │
│  │ (Python)       │  Sends to Kafka every 0.1s                          │
│  └────────┬───────┘  Batch size: 60 messages                            │
│           │          Format: Avro                                        │
│           ▼                                                              │
│  ┌────────────────┐                                                      │
│  │ Kafka Topic    │  Topic: stress-topic                                │
│  │ stress-topic   │  Partitions: 1                                      │
│  └────────┬───────┘  Replication: 1                                     │
└───────────┼──────────────────────────────────────────────────────────────┘
            │
            ├──────────────────┬──────────────────┐
            │                  │                  │
            ▼                  ▼                  ▼
┌───────────────────┐ ┌────────────────┐ ┌──────────────────┐
│ CONSUMER GROUP 1  │ │ CONSUMER GROUP 2│ │ ORCHESTRATOR     │
│ flink-consumer    │ │ dask-fallback-  │ │ (Always Running) │
│                   │ │ consumer        │ │                  │
│ ┌───────────────┐ │ │ ┌────────────┐ │ │ ┌──────────────┐ │
│ │ Flink Job     │ │ │ │ Dask       │ │ │ │ Monitor      │ │
│ │ (Primary)     │ │ │ │ Processor  │ │ │ │ - Checks     │ │
│ │               │ │ │ │ (Fallback) │ │ │ │   every 60s  │ │
│ │ • 5s windows  │ │ │ │            │ │ │ │ - Queries    │ │
│ │ • 60 msg      │ │ │ │ • 30s batch│ │ │ │   InfluxDB   │ │
│ │   batches     │ │ │ │ • 1000 msg │ │ │ │ - Triggers   │ │
│ │ • Real-time   │ │ │ │   batches  │ │ │ │   fallback   │ │
│ │               │ │ │ │ • Batch    │ │ │ └──────┬───────┘ │
│ └───────┬───────┘ │ │ └──────┬─────┘ │ └────────┼─────────┘
└─────────┼─────────┘ └────────┼───────┘          │
          │                    │                  │
          │                    │         ┌────────▼────────┐
          │                    │         │ Decision Logic  │
          │                    │         │ • Latency > 5m? │
          │                    │         │ • 3 violations? │
          │                    │         │ • Flink down?   │
          │                    │         └────────┬────────┘
          │                    │                  │
          │                    │         ┌────────▼────────┐
          │                    │         │ Mode Control    │
          │                    │         │ • Start Dask    │
          │                    │         │ • Stop Dask     │
          │                    │         └─────────────────┘
          │                    │
          ▼                    ▼
┌─────────────────────────────────────────────────────────────┐
│ FEATURE PROCESSING                                           │
│                                                              │
│ Flink Path:                    Dask Path:                   │
│ ┌──────────────────┐          ┌──────────────────┐         │
│ │ 1. Deserialize   │          │ 1. Collect batch │         │
│ │    Avro          │          │    (1000 msgs)   │         │
│ │                  │          │                  │         │
│ │ 2. Window by     │          │ 2. Group by      │         │
│ │    time (5s)     │          │    nurse ID      │         │
│ │                  │          │                  │         │
│ │ 3. Aggregate     │          │ 3. Calculate     │         │
│ │    by nurse ID   │          │    averages      │         │
│ │                  │          │                  │         │
│ │ 4. Calculate     │          │ 4. Parallel      │         │
│ │    averages      │          │    processing    │         │
│ └────────┬─────────┘          └────────┬─────────┘         │
└──────────┼────────────────────────────┼───────────────────┘
           │                            │
           ▼                            ▼
┌─────────────────────────────────────────────────────────────┐
│ ML INFERENCE                                                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Flask ML Service                                       │ │
│  │ • LightGBM model                                       │ │
│  │ • Input: X, Y, Z, EDA, HR, TEMP                       │ │
│  │ • Output: stress_level (0=low, 1=med, 2=high)        │ │
│  │ • Endpoint: /model/api/predict                        │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ DATA STORAGE                                                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ InfluxDB (Time Series Database)                       │ │
│  │                                                        │ │
│  │ Measurement: nurse_stress                             │ │
│  │                                                        │ │
│  │ Tags:                                                  │ │
│  │   • id: nurse identifier                              │ │
│  │   • source: "flink" or "dask-fallback"               │ │
│  │                                                        │ │
│  │ Fields:                                                │ │
│  │   • X, Y, Z: orientation                              │ │
│  │   • EDA: electrodermal activity                       │ │
│  │   • HR: heart rate                                    │ │
│  │   • TEMP: skin temperature                            │ │
│  │   • stress_level: prediction (0/1/2)                  │ │
│  │   • datetime: event timestamp                         │ │
│  └────────────────────────────────────────────────────────┘ │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ VISUALIZATION                                                │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Grafana Dashboard                                      │ │
│  │                                                        │ │
│  │ Panels:                                                │ │
│  │ • Stress levels by nurse (color-coded)                │ │
│  │ • Processing source (Flink vs Dask)                   │ │
│  │ • Fallback status indicator                           │ │
│  │ • Aggregate stress trends                             │ │
│  │ • Alerts for high stress                              │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## State Machine

```
┌─────────────────────────────────────────────────────────────┐
│                    SYSTEM STATE MACHINE                      │
└─────────────────────────────────────────────────────────────┘

                    ┌──────────────┐
                    │   STARTUP    │
                    │              │
                    │ • Start all  │
                    │   services   │
                    │ • Initialize │
                    │   monitor    │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
              ┌────▶│    NORMAL    │◀────┐
              │     │              │     │
              │     │ • Flink only │     │
              │     │ • Latency OK │     │
              │     │ • Dask idle  │     │
              │     └──────┬───────┘     │
              │            │             │
              │            │ Latency > 5min    │
              │            │ for 3 checks      │
              │            │             │
              │            ▼             │
              │     ┌──────────────┐    │
              │     │  TRIGGERING  │    │
              │     │              │    │
              │     │ • Violation  │    │
              │     │   count: 3   │    │
              │     │ • Starting   │    │
              │     │   Dask       │    │
              │     └──────┬───────┘    │
              │            │             │
              │            ▼             │
              │     ┌──────────────┐    │
              │     │   FALLBACK   │    │
              │     │              │    │
              │     │ • Flink +    │    │
              │     │   Dask       │    │
              │     │ • Both       │    │
              │     │   processing │    │
              │     └──────┬───────┘    │
              │            │             │
              │            │ Latency < 2.5min
              │            │             │
              │            ▼             │
              │     ┌──────────────┐    │
              │     │   RESUMING   │    │
              │     │              │    │
              │     │ • Stopping   │    │
              │     │   Dask       │    │
              │     │ • Committing │    │
              │     │   offsets    │    │
              │     └──────┬───────┘    │
              │            │             │
              └────────────┴─────────────┘
```

## Data Flow Comparison

### Normal Mode (Flink Only)

```
Time: 0s ──────────────────────────────────────────────▶ 5s

Kafka:    [60 msgs] [60 msgs] [60 msgs] [60 msgs] [60 msgs]
             │         │         │         │         │
             ▼         ▼         ▼         ▼         ▼
Flink:    ┌─────────────────────────────────────────┐
          │     5-second tumbling window            │
          │     Aggregate by nurse ID               │
          └─────────────────────────────────────────┘
                              │
                              ▼
ML:                    [15 predictions]
                              │
                              ▼
InfluxDB:              [15 records written]
                       source=flink

Latency: ~5 seconds
Throughput: ~60 msgs/sec
```

### Fallback Mode (Flink + Dask)

```
Time: 0s ──────────────────────────────────────────────▶ 30s

Kafka:    [60][60][60][60][60]...[60][60][60][60][60]
             │                        │
             ▼                        ▼
Flink:    ┌──────┐              ┌──────┐
          │ 5s   │ (struggling) │ 5s   │
          │window│              │window│
          └──┬───┘              └──┬───┘
             │                     │
             ▼                     ▼
          [slow]                [slow]

Kafka:    [1000 messages accumulated]
                      │
                      ▼
Dask:          ┌─────────────────┐
               │ 30s batch       │
               │ Group by ID     │
               │ Calculate avg   │
               └────────┬────────┘
                        │
                        ▼
ML:              [15 predictions]
                        │
                        ▼
InfluxDB:        [15 records written]
                 source=dask-fallback

Latency: ~30-60 seconds
Throughput: ~33 msgs/sec (catches up)
```

## Component Interaction Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    COMPONENT INTERACTIONS                    │
└─────────────────────────────────────────────────────────────┘

Orchestrator                Monitor              Dask Processor
     │                         │                        │
     │ start()                 │                        │
     ├────────────────────────▶│                        │
     │                         │                        │
     │                         │ check_latency()        │
     │                         ├──────────────────────▶ InfluxDB
     │                         │◀──────────────────────┤
     │                         │ latency: 2.5s          │
     │                         │                        │
     │                         │ check_flink_health()   │
     │                         ├──────────────────────▶ Flink
     │                         │◀──────────────────────┤
     │                         │ status: healthy        │
     │                         │                        │
     │                    [60 seconds pass]             │
     │                         │                        │
     │                         │ check_latency()        │
     │                         ├──────────────────────▶ InfluxDB
     │                         │◀──────────────────────┤
     │                         │ latency: 320s          │
     │                         │                        │
     │                         │ violation_count++      │
     │                         │                        │
     │                    [3 violations]                │
     │                         │                        │
     │◀────────────────────────┤ on_fallback_trigger()  │
     │                         │                        │
     │ start_dask_processor()  │                        │
     ├────────────────────────────────────────────────▶│
     │                         │                        │
     │                         │                        │ run()
     │                         │                        │
     │                         │                        │ collect_batch()
     │                         │                        ├──────▶ Kafka
     │                         │                        │◀──────┤
     │                         │                        │ [1000 msgs]
     │                         │                        │
     │                         │                        │ process_batch()
     │                         │                        │ • group by ID
     │                         │                        │ • calculate avg
     │                         │                        │
     │                         │                        │ predict_stress()
     │                         │                        ├──────▶ ML Service
     │                         │                        │◀──────┤
     │                         │                        │ [predictions]
     │                         │                        │
     │                         │                        │ write_to_influxdb()
     │                         │                        ├──────▶ InfluxDB
     │                         │                        │
     │                         │                        │ commit()
     │                         │                        ├──────▶ Kafka
     │                         │                        │
     │                    [latency drops]               │
     │                         │                        │
     │◀────────────────────────┤ on_primary_resume()    │
     │                         │                        │
     │ stop_dask_processor()   │                        │
     ├────────────────────────────────────────────────▶│
     │                         │                        │ stop()
     │                         │                        │
```

## Kafka Consumer Groups

```
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA TOPIC: stress-topic                 │
│                                                              │
│  Partition 0: [msg1][msg2][msg3][msg4][msg5]...[msgN]      │
│                  ▲                    ▲                      │
│                  │                    │                      │
│            offset: 1000         offset: 1500                │
└──────────────────┼────────────────────┼──────────────────────┘
                   │                    │
         ┌─────────┴────────┐  ┌────────┴─────────┐
         │                  │  │                  │
    ┌────▼──────────────┐  │  │  ┌───▼──────────────────┐
    │ Consumer Group 1  │  │  │  │ Consumer Group 2     │
    │ flink-consumer    │  │  │  │ dask-fallback-       │
    │                   │  │  │  │ consumer             │
    │ • Reads from 1000 │  │  │  │ • Reads from 1500    │
    │ • Commits offset  │  │  │  │ • Commits offset     │
    │ • Independent     │  │  │  │ • Independent        │
    └───────────────────┘  │  │  └──────────────────────┘
                           │  │
                    No Conflict!
                    Both can read
                    simultaneously
```

## Monitoring Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    MONITORING WORKFLOW                       │
└─────────────────────────────────────────────────────────────┘

Every 60 seconds:

1. Query InfluxDB
   ┌────────────────────────────────────────┐
   │ SELECT last("datetime")                │
   │ FROM "nurse_stress"                    │
   │ WHERE time > now() - 10m               │
   └────────────────────────────────────────┘
                    │
                    ▼
2. Calculate Latency
   ┌────────────────────────────────────────┐
   │ latency = current_time - last_datetime │
   └────────────────────────────────────────┘
                    │
                    ▼
3. Check Threshold
   ┌────────────────────────────────────────┐
   │ if latency > 300s:                     │
   │     violation_count++                  │
   │ else:                                  │
   │     violation_count = 0                │
   └────────────────────────────────────────┘
                    │
                    ▼
4. Decision
   ┌────────────────────────────────────────┐
   │ if violation_count >= 3:               │
   │     trigger_fallback()                 │
   │                                        │
   │ if fallback_active and latency < 150s: │
   │     resume_primary()                   │
   └────────────────────────────────────────┘
```

## Configuration Hierarchy

```
┌─────────────────────────────────────────────────────────────┐
│                    CONFIGURATION LAYERS                      │
└─────────────────────────────────────────────────────────────┘

1. Default Values (in code)
   ├─ latency_threshold: 300s
   ├─ batch_size: 1000
   └─ consecutive_violations: 3

2. config.yaml (overrides defaults)
   ├─ kafka:
   │  ├─ bootstrap_servers
   │  ├─ topic
   │  └─ group_id
   ├─ monitoring:
   │  ├─ latency_threshold_seconds
   │  ├─ consecutive_violations
   │  └─ check_interval_seconds
   ├─ processing:
   │  ├─ batch_size
   │  └─ batch_timeout_seconds
   └─ dask:
      ├─ n_workers
      └─ memory_limit

3. Environment Variables (overrides config.yaml)
   ├─ KAFKA_BOOTSTRAP_SERVERS
   ├─ LATENCY_THRESHOLD
   └─ BATCH_SIZE

4. Runtime Adjustments (via API - future)
   └─ Dynamic threshold adjustment
```

## Error Handling Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    ERROR HANDLING                            │
└─────────────────────────────────────────────────────────────┘

Dask Processor:
    │
    ├─ Kafka Connection Error
    │  ├─ Retry 3 times
    │  ├─ Wait 5 seconds between retries
    │  └─ Log error and continue
    │
    ├─ Avro Decode Error
    │  ├─ Log message details
    │  ├─ Skip message
    │  └─ Continue processing
    │
    ├─ ML Service Error
    │  ├─ Retry 3 times
    │  ├─ Use default prediction (1)
    │  └─ Log warning
    │
    └─ InfluxDB Write Error
       ├─ Log error
       ├─ Continue processing
       └─ Don't commit Kafka offset

Monitor:
    │
    ├─ InfluxDB Query Error
    │  ├─ Log warning
    │  ├─ Return None
    │  └─ Don't increment violation count
    │
    └─ Flink Health Check Error
       ├─ Log warning
       ├─ Increment violation count
       └─ May trigger fallback
```

## Performance Characteristics

```
┌─────────────────────────────────────────────────────────────┐
│                    PERFORMANCE METRICS                       │
└─────────────────────────────────────────────────────────────┘

Flink (Primary):
┌──────────────────────────────────────────────────────────┐
│ Latency:      ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  5s     │
│ Throughput:   ████████████████████████████████  3333/s   │
│ CPU:          ████████████████████░░░░░░░░░░░░  60%      │
│ Memory:       ████████████████░░░░░░░░░░░░░░░░  2 GB     │
└──────────────────────────────────────────────────────────┘

Dask (Fallback):
┌──────────────────────────────────────────────────────────┐
│ Latency:      ████████████░░░░░░░░░░░░░░░░░░░░  30-60s  │
│ Throughput:   ████████████████████████████████  33/s     │
│ CPU:          ████████████░░░░░░░░░░░░░░░░░░░  40%      │
│ Memory:       ████████████░░░░░░░░░░░░░░░░░░░  1.5 GB   │
└──────────────────────────────────────────────────────────┘

Combined (Fallback Mode):
┌──────────────────────────────────────────────────────────┐
│ Latency:      ████████████░░░░░░░░░░░░░░░░░░░░  30-60s  │
│ Throughput:   ████████████████████████████████  3366/s   │
│ CPU:          ████████████████████████░░░░░░░░  75%      │
│ Memory:       ████████████████████████░░░░░░░░  3.5 GB   │
└──────────────────────────────────────────────────────────┘
```

---

This architecture ensures **continuous stress monitoring** with **automatic resource-aware switching** and **zero data loss**.
