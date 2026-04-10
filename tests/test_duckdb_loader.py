import importlib
import sys
from types import SimpleNamespace

import duckdb


def test_duckdb_loader_creates_tables(tmp_path, monkeypatch):
    # Prepare a temporary duckdb file path
    db_path = str(tmp_path / "test.duckdb")

    # Create a fake config object with required attributes
    fake_cfg = SimpleNamespace(
        GOOGLE_CLOUD_PROJECT="fake-project",
        GCS_BUCKET_NAME="fake-bucket",
        DUCKDB_DATABASE=db_path,
        BIGQUERY_TABLE_BTC_TRADES="btc_trades",
        BIGQUERY_TRACKING_TABLE="loaded_files",
    )

    # Patch config.load_config to return our fake config before importing the module
    monkeypatch.setattr("config.load_config", lambda: fake_cfg)

    # Patch storage.Client so the module's GCS listing doesn't call real GCS
    class FakeBlob:
        def __init__(self, name):
            self.name = name

    class FakeStorageClient:
        def __init__(self, project=None):
            pass

        def list_blobs(self, bucket, prefix=None):
            # yield one fake blob so the loader doesn't abort
            yield FakeBlob(f"{prefix}sample.parquet")

    monkeypatch.setattr("google.cloud.storage.Client", FakeStorageClient)

    # Ensure module is not already imported (so import will execute loader code)
    mod_name = "loaders.duckdb_loader"
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    # Import the module which should create the DuckDB file and tables
    mod = importlib.import_module(mod_name)

    # Connect to the created DuckDB file and check tables
    conn = duckdb.connect(db_path)
    rows = conn.execute("SHOW TABLES").fetchall()
    table_names = {r[0] for r in rows}

    assert fake_cfg.BIGQUERY_TABLE_BTC_TRADES in table_names
    assert fake_cfg.BIGQUERY_TRACKING_TABLE in table_names

    conn.close()
