"""GCS sync helpers for the Spark Bronze ingestion job."""
import json
import os
from datetime import datetime

from google.cloud import storage

WATERMARK_BLOB = "bronze/_state/watermark.json"


def _client(project=None):
    return storage.Client(project=project) if project else storage.Client()


def read_watermark(bucket_name, project=None):
    blob = _client(project).bucket(bucket_name).blob(WATERMARK_BLOB)
    if not blob.exists():
        return None
    return datetime.fromisoformat(json.loads(blob.download_as_text())["last_processed_at"])


def write_watermark(bucket_name, when, project=None):
    blob = _client(project).bucket(bucket_name).blob(WATERMARK_BLOB)
    blob.upload_from_string(json.dumps({"last_processed_at": when.isoformat()}))


def download_new_blobs(bucket_name, prefix, staging_dir, since=None, project=None):
    client = _client(project)
    os.makedirs(staging_dir, exist_ok=True)
    local_paths, latest = [], since
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if not blob.name.endswith(".parquet"):
            continue
        if since is not None and blob.time_created <= since:
            continue
        dest = os.path.join(staging_dir, os.path.basename(blob.name))
        blob.download_to_filename(dest)
        local_paths.append(dest)
        if latest is None or blob.time_created > latest:
            latest = blob.time_created
    return local_paths, latest


def upload_output(local_dir, bucket_name, dest_prefix, project=None):
    bucket = _client(project).bucket(bucket_name)
    uploaded = 0
    for root, _dirs, files in os.walk(local_dir):
        for fname in files:
            if not fname.endswith(".parquet"):
                continue
            local_path = os.path.join(root, fname)
            rel = os.path.relpath(local_path, local_dir)
            bucket.blob(f"{dest_prefix}/{rel}").upload_from_filename(local_path)
            uploaded += 1
    return uploaded