import duckdb
from google.cloud import storage
from config import load_config
import tempfile
import os

cfg = load_config()

project_id = cfg.GOOGLE_CLOUD_PROJECT
bucket_name = cfg.GCS_BUCKET_NAME
duckdb_path = cfg.DUCKDB_DATABASE or "analytics.duckdb"
# Create main table using centralized config names
main_table = cfg.BIGQUERY_TABLE_BTC_TRADES or "btc_trades"
tracking_table = cfg.BIGQUERY_TRACKING_TABLE or "loaded_files"

conn = duckdb.connect(duckdb_path)

print("📡 Loading data from GCS into DuckDB...")

conn.execute(f"""
CREATE TABLE IF NOT EXISTS {main_table} (
    symbol VARCHAR,
    price DOUBLE,
    quantity DOUBLE,
    timestamp TIMESTAMP
)
""")

# Create tracking table (same idea as BigQuery)
conn.execute(f"""
CREATE TABLE IF NOT EXISTS {tracking_table} (
    file_name VARCHAR PRIMARY KEY
)
""")

try:
    storage_client = storage.Client(project=project_id) if project_id else storage.Client()
    prefix = "btc_trades/"
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
    # Optional limit for safe/dry-run: set DUCKDB_LIMIT in env to an integer
    try:
        import os

        limit = int(os.getenv("DUCKDB_LIMIT")) if os.getenv("DUCKDB_LIMIT") else None
    except Exception:
        limit = None
    if limit:
        print(f"Limiting processing to first {limit} files (DUCKDB_LIMIT set)")
        blobs = blobs[:limit]
    if not blobs:
        print(f"No objects found in gs://{bucket_name}/{prefix}. Aborting.")
        raise SystemExit(1)
    else:
        print(f"Found {len(blobs)} objects in gs://{bucket_name}/{prefix}.")
except Exception as exc:
    print("Failed to list GCS objects (check credentials/permissions):", exc)
    raise SystemExit(1)

for blob in blobs:
    file_name = blob.name
    print(f"Processing {file_name}...")
    # Check if this file has already been loaded (tracking table)
    res = conn.execute(f"SELECT 1 FROM {tracking_table} WHERE file_name = ?", (file_name,)).fetchone()
    if res:
        print(f"Already loaded {file_name}, skipping.")
        continue

    # Download the object locally and read via DuckDB to avoid HTTP auth issues
    try:
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_path = tmp.name
        tmp.close()
        bucket = storage_client.bucket(bucket_name)
        blob_obj = bucket.blob(file_name)
        blob_obj.download_to_filename(tmp_path)

        try:
            conn.execute(f"""
            INSERT INTO {main_table} (symbol, price, quantity, timestamp)
            SELECT symbol, price, quantity, timestamp
            FROM read_parquet('{tmp_path}')
            """)
        except Exception as exc_inner:
            errstr = str(exc_inner)
            # Fallback: some Parquet files store timestamp as INT64 milliseconds.
            # Detect the BIGINT->TIMESTAMP conversion error and retry converting ms -> seconds.
            if ("BIGINT -> TIMESTAMP" in errstr) or ("Unimplemented type for cast" in errstr):
                try:
                    conn.execute(f"""
                    INSERT INTO {main_table} (symbol, price, quantity, timestamp)
                    SELECT symbol, price, quantity, to_timestamp(timestamp/1000)
                    FROM read_parquet('{tmp_path}')
                    """)
                except Exception as exc_retry:
                    print(f"Failed to load {file_name} on retry with ms->timestamp conversion: {exc_retry}")
                    raise
            else:
                print(f"Failed to load {file_name}: {exc_inner}")
                raise

        # Mark this file as loaded in the tracking table
        conn.execute(f"INSERT INTO {tracking_table} (file_name) VALUES (?)", (file_name,))
        print(f"Successfully loaded {file_name} into DuckDB.")
    except Exception as exc:
        print(f"Failed to load {file_name}: {exc}")
    finally:
        # Clean up temporary file if it exists
        try:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        
print("✅ DuckDB load complete")