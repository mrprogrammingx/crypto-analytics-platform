import types
import datetime
from unittest import mock

import loaders.bigquery_loader as bq_loader


class FakeBlob:
    def __init__(self, name):
        self.name = name


class FakeStorageClient:
    def __init__(self, blobs):
        # blobs: list of blob names (strings)
        self._blobs = [FakeBlob(n) for n in blobs]

    def list_blobs(self, bucket, prefix=None):
        # return an iterator
        for b in self._blobs:
            yield b


class FakeLoadJob:
    def __init__(self, output_rows=100, errors=None):
        self.output_rows = output_rows
        self.errors = errors or None

    def result(self):
        return None


class FakeBigQueryClient:
    def __init__(self, project="fake-project", already_loaded=None):
        self.project = project
        # set of already recorded URIs
        self.already_loaded = set(already_loaded or [])
        self.loaded_uris = None
        self.recorded_rows = None

    def get_table(self, table_id):
        # assume tracking table exists for this integration test
        return object()

    def query(self, sql):
        # emulate returning rows with attribute file_name
        rows = [types.SimpleNamespace(file_name=f) for f in self.already_loaded]
        return types.SimpleNamespace(result=lambda: rows)

    def load_table_from_uri(self, uris, table_id, job_config=None):
        # record the URIs passed in and return a fake job
        self.loaded_uris = list(uris)
        return FakeLoadJob(output_rows=42)

    def insert_rows_json(self, table_id, rows):
        # record what was inserted and return [] for success
        self.recorded_rows = rows
        return []


def test_integration_loads_only_new_and_records(tmp_path):
    # Prepare fake blobs in storage
    bucket = "my-bucket"
    blobs = [
        "btc_trades/year=2026/month=4/day=4/btc_trades_1.parquet",
        "btc_trades/year=2026/month=4/day=4/btc_trades_2.parquet",
    ]

    # Patch module-level storage_client and client with fakes
    fake_storage = FakeStorageClient(blobs)
    # Pretend file 1 was already loaded
    already = [f"gs://{bucket}/{blobs[0]}"]
    fake_bq = FakeBigQueryClient(project="fake-project", already_loaded=already)

    bq_loader.storage_client = fake_storage
    bq_loader.client = fake_bq

    # Set GCS_URI to the wildcard pattern used by loader but for our fake bucket
    bq_loader.GCS_URI = f"gs://{bucket}/btc_trades/year=*/month=*/day=*/*.parquet"

    # Run main() — should load only the second file
    bq_loader.main()

    assert fake_bq.loaded_uris is not None
    assert len(fake_bq.loaded_uris) == 1
    assert fake_bq.loaded_uris[0].endswith("btc_trades_2.parquet")

    # Verify the tracking table recorded one row corresponding to the loaded URI
    assert fake_bq.recorded_rows is not None
    assert len(fake_bq.recorded_rows) == 1
    assert fake_bq.recorded_rows[0]["file_name"].endswith("btc_trades_2.parquet")
    # loaded_at should be a datetime
    assert isinstance(fake_bq.recorded_rows[0]["loaded_at"], datetime.datetime)
