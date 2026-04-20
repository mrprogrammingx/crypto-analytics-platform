from kafka import KafkaConsumer
import json
import pandas as pd
from transformations.clean import clean_trade
from transformations.validators import is_valid_trade
from config import load_config
from google.cloud import storage
from datetime import datetime, timezone
import uuid
import os

consuner = KafkaConsumer(
    'btc_trades',
    bootstrap_servers='localhost:9092',
    group_id='btc_trades_parquet_gcs_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


cfg = load_config()
bucket_name = cfg.GCS_BUCKET_NAME
BATCH_SIZE = cfg.BATCH_SIZE or 100  # Default batch size if not set in .env
if not bucket_name:
    print("GCS_BUCKET_NAME is not set in the environment (.env). Aborting.")
    exit(1)
    
try:
    # If the user provided a GCP project via env, pass it to the client constructor.
    client_kwargs = {}
    if cfg.GOOGLE_CLOUD_PROJECT:
        client_kwargs["project"] = cfg.GOOGLE_CLOUD_PROJECT
    storage_client = storage.Client(**client_kwargs)
except Exception as exc:
    print("Failed to create GCS client:", exc)
    print("Make sure GOOGLE_APPLICATION_CREDENTIALS is set or that gcloud is authenticated, and set GOOGLE_CLOUD_PROJECT in .env if necessary.")
    raise
# storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

print("📡 Listening and storing data in GCS as Parquet...")

buffer = []

for msg in consuner:
    raw = msg.value
    print("Raw trade data:", raw)
    
    if not is_valid_trade(raw):
        print("Invalid trade data:", raw)
        continue

    cleaned = clean_trade(raw)
    if cleaned:
        buffer.append(cleaned)

    if len(buffer) >= BATCH_SIZE:
        df = pd.DataFrame(buffer)
        # Convert timestamp (ms since epoch) to pandas timezone-aware datetime so
        # Parquet preserves it and BigQuery LOAD jobs can infer TIMESTAMP correctly.
        if "timestamp" in df.columns:
            try:
                # timestamps are expected in milliseconds
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
            except Exception:
                # fallback: try to coerce without unit
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        # Use timezone-aware UTC datetimes to avoid deprecation warnings
        now = datetime.now(timezone.utc)
        filename = f"btc_trades_{now.strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
        path = f"btc_trades/year={now.year}/month={now.month}/day={now.day}/"
        local_file = f"/tmp/{filename}"

        df.to_parquet(local_file, index=False)

        blob = bucket.blob(path + filename)
        blob.upload_from_filename(local_file)
        print(f"Uploaded {len(buffer)} records to GCS as {filename}")

        os.remove(local_file)
        buffer.clear()