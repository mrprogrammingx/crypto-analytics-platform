from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import SchemaField, Table

from config import load_config

import fnmatch
import datetime

cfg = load_config()

PROJECT_ID = cfg.GOOGLE_CLOUD_PROJECT
bucket_name = cfg.GCS_BUCKET_NAME
# Centralized dataset/table names from config
DATASET = cfg.BIGQUERY_DATASET
TABLE = cfg.BIGQUERY_TABLE_BTC_TRADES
TRACKING_TABLE = cfg.BIGQUERY_TRACKING_TABLE

try:
    # If the user provided a GCP project via env, pass it to the client constructor.
    client_kwargs = {}
    # Use the general project config if available (covers BigQuery/GCS/others)
    if PROJECT_ID:
        client_kwargs["project"] = PROJECT_ID
    client = bigquery.Client(**client_kwargs)
except Exception as exc:
    print("Failed to create BigQuery client:", exc)
    print("Make sure GOOGLE_APPLICATION_CREDENTIALS is set or that gcloud is authenticated, and set GOOGLE_CLOUD_PROJECT in .env if necessary.")
    raise


# Build table_id from centralized config (fall back to BIGQUERY_TABLE_ID)
if cfg.BIGQUERY_TABLE_ID:
    table_id = cfg.BIGQUERY_TABLE_ID
else:
    # prefer project.dataset.table when project is set
    if PROJECT_ID:
        table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    else:
        table_id = f"{DATASET}.{TABLE}"

# GCS path (can be wildcard). Use TABLE value as the prefix directory.
GCS_URI = f"gs://{bucket_name}/{TABLE}/year=*/month=*/day=*/*.parquet"

# Pre-check using the Storage client: ensure at least one object exists under the prefix
try:
    storage_client = storage.Client(project=PROJECT_ID) if PROJECT_ID else storage.Client()
    prefix = "btc_trades/"
    blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix, max_results=1))
    if not blobs:
        print(f"No objects found in gs://{bucket_name}/{prefix}. Aborting load.")
        raise SystemExit(1)
    else:
        print(f"Found object example: {blobs[0].name}")
except Exception as exc:
    print("Failed to list GCS objects (check credentials/permissions):", exc)
    raise

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # append data
)

print("🚀 Starting BigQuery Load Job...")


def _get_tracking_table_id(dest_table_id: str, bq_client: bigquery.Client) -> str:
    """Return a fully-qualified tracking table id for loaded_files in the same dataset as dest_table_id."""
    parts = dest_table_id.split(".")
    if len(parts) == 3:
        project = parts[0]
        dataset = parts[1]
    elif len(parts) == 2:
        project = bq_client.project
        dataset = parts[0]
    else:
        raise SystemExit(f"Invalid destination table id: {dest_table_id}")
    # Use centralized tracking table name if available
    tracking = TRACKING_TABLE or "loaded_files"
    return f"{project}.{dataset}.{tracking}"


def _ensure_tracking_table(tracking_table_id: str, bq_client: bigquery.Client):
    """Create the tracking table if it doesn't exist."""
    try:
        bq_client.get_table(tracking_table_id)
        # exists
    except NotFound:
        print(
            f"Tracking table {tracking_table_id} not found. Please run `./scripts/create_bigquery_table.py --tracking` to create it, then re-run this loader."
        )
        raise SystemExit(1)


def _get_already_loaded_files(tracking_table_id: str, bq_client: bigquery.Client) -> set:
    """Return a set of file_name strings already recorded in the tracking table."""
    try:
        query = f"SELECT file_name FROM `{tracking_table_id}`"
        rows = bq_client.query(query).result()
        return {row.file_name for row in rows}
    except Exception:
        # If the table does not exist or query fails, treat as empty set
        return set()


def main():
    uris_to_load = []
    if "*" in GCS_URI:
        # parse bucket and pattern
        # GCS_URI is like 'gs://bucket/some/prefix/*/file-*.parquet'
        uri_body = GCS_URI[len("gs://") :]
        first_slash = uri_body.find("/")
        if first_slash == -1:
            print("Invalid GCS_URI format")
            raise SystemExit(1)
        bucket_for_pattern = uri_body[:first_slash]
        pattern = uri_body[first_slash+1:]

        # Determine a root prefix to list (up to first wildcard)
        wildcard_idx = min([pattern.find(c) for c in ['*', '?'] if c in pattern] + [len(pattern)])
        root_prefix = pattern[:pattern.rfind('/', 0, wildcard_idx) + 1] if '/' in pattern[:wildcard_idx] else ''

        print(f"Expanding pattern gs://{bucket_for_pattern}/{pattern} (listing prefix: {root_prefix})")
        blobs = storage_client.list_blobs(bucket_for_pattern, prefix=root_prefix)
        matches = []
        for b in blobs:
            if fnmatch.fnmatch(b.name, pattern):
                matches.append(f"gs://{bucket_for_pattern}/{b.name}")

        if not matches:
            print(f"No objects matched pattern {pattern} in bucket {bucket_for_pattern}. Aborting.")
            raise SystemExit(1)

        print(f"Found {len(matches)} files to potentially load (showing up to 5):")
        for m in matches[:5]:
            print(" -", m)

        uris_to_load = matches
    else:
        uris_to_load = [GCS_URI]

    # Prepare and ensure tracking table
    tracking_table_id = _get_tracking_table_id(table_id, client)
    _ensure_tracking_table(tracking_table_id, client)

    # Filter out files we've already loaded
    already_loaded = _get_already_loaded_files(tracking_table_id, client)
    if already_loaded:
        before = len(uris_to_load)
        uris_to_load = [u for u in uris_to_load if u not in already_loaded]
        skipped = before - len(uris_to_load)
        if skipped:
            print(f"Skipping {skipped} already-loaded files.")

    if not uris_to_load:
        print("No new files to load after filtering; exiting.")
        raise SystemExit(0)

    print(f"Loading {len(uris_to_load)} files...")

    load_job = client.load_table_from_uri(
        uris_to_load,
        table_id,
        job_config=job_config,
    )

    load_job.result()  # wait for job to complete

    if load_job.errors:
        print("BigQuery load job completed with errors:", load_job.errors)
        raise SystemExit(1)

    print(f"✅ Loaded {load_job.output_rows} rows into {table_id} from {len(uris_to_load)} files")

    # Record loaded files in the tracking table
    rows_to_insert = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # insert_rows_json expects JSON-serializable values; serialize timestamp to ISO 8601
    now_iso = now_utc.isoformat()
    for uri in uris_to_load:
        rows_to_insert.append({"file_name": uri, "loaded_at": now_iso})

    insert_errors = client.insert_rows_json(tracking_table_id, rows_to_insert)
    if insert_errors:
        print("Warning: failed to record some loaded files:", insert_errors)
    else:
        print(f"Recorded {len(rows_to_insert)} files in {tracking_table_id}")


if __name__ == "__main__":
    main()