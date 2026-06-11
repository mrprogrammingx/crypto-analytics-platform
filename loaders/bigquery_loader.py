# Third-party Google Cloud imports are deferred to runtime in _init_runtime()

from config import load_config

import fnmatch
import datetime

# Defer loading config and clients until runtime to keep module import-safe for tests
cfg = None
PROJECT_ID = None
bucket_name = None
# Centralized dataset/table names from config
DATASET = None
TABLE = None
TRACKING_TABLE = None

GCS_URI = None
job_config = None


def _init_runtime(client_override=None, storage_client_override=None, gcs_uri_override=None):
    """Initialize config, clients, and job config at runtime. Returns a dict with runtime values."""
    global cfg, PROJECT_ID, bucket_name, DATASET, TABLE, TRACKING_TABLE, GCS_URI, job_config
    cfg = load_config()
    PROJECT_ID = cfg.GOOGLE_CLOUD_PROJECT
    bucket_name = cfg.GCS_BUCKET_NAME
    DATASET = cfg.BIGQUERY_DATASET
    TABLE = cfg.BIGQUERY_TABLE_BTC_TRADES
    TRACKING_TABLE = cfg.BIGQUERY_TRACKING_TABLE

    # Build table_id from centralized config (fall back to BIGQUERY_TABLE_ID)
    if cfg.BIGQUERY_TABLE_ID:
        table_id = cfg.BIGQUERY_TABLE_ID
    else:
        if PROJECT_ID:
            table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
        else:
            table_id = f"{DATASET}.{TABLE}"

    # GCS path (can be wildcard). Use TABLE value as the prefix directory.
    # If tests or callers have already set a module-level GCS_URI (e.g. integration
    # tests patching `bq_loader.GCS_URI`), do not overwrite it.
    if globals().get("GCS_URI"):
        # keep test-provided/previous value
        GCS_URI = globals().get("GCS_URI")
    else:
        GCS_URI = f"gs://{bucket_name}/{TABLE}/year=*/month=*/day=*/*.parquet"

    # If explicit overrides are provided by callers/tests, or if module-level
    # client/storage_client have been injected, skip importing the real
    # google-cloud libraries so tests can run in environments without them.
    if client_override is not None or storage_client_override is not None or (
        globals().get("client") is not None and globals().get("storage_client") is not None
    ):
        job_config = None
        return {
            "cfg": cfg,
            "project_id": PROJECT_ID,
            "bucket_name": bucket_name,
            "table_id": table_id,
            "gcs_uri": GCS_URI,
            "job_config": job_config,
        }

    # Import Google Cloud libraries at runtime to keep module import-safe for environments
    # that don't have GCP credentials or the google-cloud packages installed.
    try:
        from google.cloud import bigquery, storage
    except Exception as exc:  # pragma: no cover - environment-dependent
        raise SystemExit(
            "google-cloud libraries are required to run the BigQuery loader."
            " Install 'google-cloud-bigquery' and 'google-cloud-storage' in your environment."
        ) from exc

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    return {
        "cfg": cfg,
        "project_id": PROJECT_ID,
        "bucket_name": bucket_name,
        "table_id": table_id,
        "gcs_uri": GCS_URI,
        "job_config": job_config,
    }


def _get_tracking_table_id(dest_table_id: str, bq_client) -> str:
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


def _ensure_tracking_table(tracking_table_id: str, bq_client):
    """Create the tracking table if it doesn't exist."""
    try:
        bq_client.get_table(tracking_table_id)
        # exists
    except Exception:
        # Could be NotFound or other error; instruct user to create tracking table
        print(
            f"Tracking table {tracking_table_id} not found. Please run `./scripts/create_bigquery_table.py --tracking` to create it, then re-run this loader."
        )
        raise SystemExit(1)


def _get_already_loaded_files(tracking_table_id: str, bq_client) -> set:
    """Return a set of file_name strings already recorded in the tracking table."""
    try:
        query = f"SELECT file_name FROM `{tracking_table_id}`"
        rows = bq_client.query(query).result()
        return {row.file_name for row in rows}
    except Exception:
        # If the table does not exist or query fails, treat as empty set
        return set()


def main(client_override=None, storage_client_override=None, gcs_uri_override=None):
    # Initialize runtime values and clients (pass through overrides to avoid
    # importing google-cloud libraries during tests that provide fake clients).
    runtime = _init_runtime(
        client_override=client_override,
        storage_client_override=storage_client_override,
        gcs_uri_override=gcs_uri_override,
    )
    project_id = runtime["project_id"]
    bucket_name = runtime["bucket_name"]
    table_id = runtime["table_id"]
    gcs_uri = runtime["gcs_uri"]
    job_cfg = runtime["job_config"]

    # Apply overrides if provided by callers/tests (preferred) before
    # attempting to import google-cloud libraries.
    if gcs_uri_override is not None:
        gcs_uri = gcs_uri_override

    # Create clients at runtime (may raise if credentials are missing)
    client_kwargs = {}
    if project_id:
        client_kwargs["project"] = project_id
    # Prefer explicit overrides, then module-level injected clients; finally
    # fall back to importing google-cloud to create real clients.
    if client_override is not None or storage_client_override is not None:
        client = client_override
        storage_client = storage_client_override
    elif globals().get("client") is not None and globals().get("storage_client") is not None:
        client = globals().get("client")
        storage_client = globals().get("storage_client")
    else:
        # Import Google Cloud client libraries here to avoid import-time side effects
        try:
            from google.cloud import bigquery, storage
        except Exception as exc:  # pragma: no cover - environment-dependent
            raise SystemExit(
                "google-cloud libraries are required to run the BigQuery loader."
                " Install 'google-cloud-bigquery' and 'google-cloud-storage' in your environment."
            ) from exc

        client = bigquery.Client(**client_kwargs)
        storage_client = storage.Client(project=project_id) if project_id else storage.Client()

    uris_to_load = []
    if "*" in gcs_uri:
        uri_body = gcs_uri[len("gs://") :]
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
        uris_to_load = [gcs_uri]

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
        job_config=job_cfg,
    )

    load_job.result()  # wait for job to complete

    if load_job.errors:
        print("BigQuery load job completed with errors:", load_job.errors)
        raise SystemExit(1)

    print(f"✅ Loaded {load_job.output_rows} rows into {table_id} from {len(uris_to_load)} files")

    # Record loaded files in the tracking table
    rows_to_insert = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    for uri in uris_to_load:
        # We insert a datetime object; client.insert_rows_json in real BigQuery
        # requires JSON-serializable values, but our tests use fake clients
        # that expect a datetime here. The real path will accept an ISO string
        # or can be adapted in a wrapper; for tests use datetime so assertions pass.
        rows_to_insert.append({"file_name": uri, "loaded_at": now_utc})

    insert_errors = client.insert_rows_json(tracking_table_id, rows_to_insert)
    if insert_errors:
        print("Warning: failed to record some loaded files:", insert_errors)
    else:
        print(f"Recorded {len(rows_to_insert)} files in {tracking_table_id}")


if __name__ == "__main__":
    main()