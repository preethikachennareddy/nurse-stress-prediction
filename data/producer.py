import os
import csv
import time
import zipfile
from datetime import datetime, timedelta
from io import BytesIO, TextIOWrapper

import avro.schema
import avro.io
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient


BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "stress-topic")

ZIP_PATH = "/data/workers.csv.zip"
# Sensor data generated every 3 ms
EVENT_INTERVAL_MS = 3
BATCH_INTERVAL_MS = 990
EVENTS_PER_BATCH_PER_ID = BATCH_INTERVAL_MS // EVENT_INTERVAL_MS   # = 330

SCHEMA_PATH = "/data/nurse_sensor_event.avsc"

START_TIME = datetime.now()


# Connect to kafka
def wait_for_bootstrap(bootstrap, timeout=3, retries=60):
    import socket
    host, port = bootstrap.split(",")[0].split(":")
    port = int(port)

    for attempt in range(retries):
        try:
            with socket.create_connection((host, port), timeout=timeout):
                print(f"Connected to Kafka at {host}:{port}")
                return
        except Exception:
            print(f"Waiting for Kafka at {host}:{port} (attempt {attempt+1})")
            time.sleep(1.0)

    raise RuntimeError(f"Could not connect to Kafka at {host}:{port}")

# Check topic exists
def ensure_topic_exists(topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    if topic_name not in admin_client.list_topics():
        raise RuntimeError(f"Topic '{topic_name}' does not exist!")
    print(f"Topic '{topic_name}' exists.")


wait_for_bootstrap(BOOTSTRAP)
ensure_topic_exists(TOPIC)

# Load AVRO schema
schema = avro.schema.parse(open(SCHEMA_PATH, "r").read())

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: v
)

def encode_avro(record, schema):
    bytes_writer = BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(record, encoder)
    return bytes_writer.getvalue()



# Read data from zip
def open_csv_reader_from_zip(zip_path, filename):
    """
    Opens a CSV inside a ZIP file and returns a streaming DictReader.
    """
    zf = zipfile.ZipFile(zip_path, 'r')
    if filename not in zf.namelist():
        raise FileNotFoundError(f"{filename} not found inside {zip_path}")

    raw = zf.open(filename, 'r')
    text_stream = TextIOWrapper(raw, encoding='utf-8')
    return zf, raw, csv.DictReader(text_stream)


# Open both CSVs from ZIP:
zip_6D, raw_6D, reader_6D = open_csv_reader_from_zip(ZIP_PATH, "6D.csv")
zip_BG, raw_BG, reader_BG = open_csv_reader_from_zip(ZIP_PATH, "BG.csv")

# Synthetic clocks
time_6D = START_TIME
time_BG = START_TIME

TIME_STEP = timedelta(milliseconds=EVENT_INTERVAL_MS)

# Get record in avro format
def get_next_records(reader, curr_time, events_needed, device_id):
    batch = []
    for _ in range(events_needed):
        try:
            row = next(reader)
        except StopIteration:
            return batch, curr_time, True

        ts_str = curr_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        avro_record = {
            "X": float(row["X"]) if row["X"] else None,
            "Y": float(row["Y"]) if row["Y"] else None,
            "Z": float(row["Z"]) if row["Z"] else None,
            "EDA": float(row["EDA"]) if row["EDA"] else None,
            "HR": float(row["HR"]) if row["HR"] else None,
            "TEMP": float(row["TEMP"]) if row["TEMP"] else None,
            "id": device_id,
            "datetime": ts_str
        }

        batch.append(avro_record)
        curr_time += TIME_STEP

    return batch, curr_time, False

# Send records for each id
def send_batches_forever():
    global time_6D, time_BG, reader_6D, reader_BG

    while True:
        batch = []

        recs_6D, time_6D, ended_6D = get_next_records(
            reader_6D, time_6D, EVENTS_PER_BATCH_PER_ID, "6D"
        )
        batch.extend(recs_6D)

        recs_BG, time_BG, ended_BG = get_next_records(
            reader_BG, time_BG, EVENTS_PER_BATCH_PER_ID, "BG"
        )
        batch.extend(recs_BG)

        for rec in batch:
            payload = encode_avro(rec, schema)
            key = rec["id"].encode("utf-8")
            producer.send(TOPIC, key=key, value=payload)

        producer.flush()

        print(f"Sent batch: {len(batch)} events")

        if ended_6D or ended_BG:
            print("CSV exhausted. Exiting.")
            return

        time.sleep(BATCH_INTERVAL_MS / 1000.0)


send_batches_forever()

# Close resources
raw_6D.close()
raw_BG.close()
zip_6D.close()
zip_BG.close()
