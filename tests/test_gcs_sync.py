from unittest.mock import MagicMock
from datetime import datetime, timezone
from spark.jobs import gcs_sync


def test_download_new_blobs_filters_by_watermark(tmp_path, monkeypatch):
    old = MagicMock(name="old.parquet", time_created=datetime(2024, 1, 1, tzinfo=timezone.utc))
    old.name = "btc_trades/old.parquet"
    new = MagicMock(name="new.parquet", time_created=datetime(2024, 6, 1, tzinfo=timezone.utc))
    new.name = "btc_trades/new.parquet"
    for b in (old, new):
        b.download_to_filename = MagicMock()

    client = MagicMock()
    client.list_blobs.return_value = [old, new]
    monkeypatch.setattr(gcs_sync, "_client", lambda project=None: client)

    since = datetime(2024, 3, 1, tzinfo=timezone.utc)
    paths, latest = gcs_sync.download_new_blobs("bucket", "btc_trades", str(tmp_path), since=since)

    assert len(paths) == 1
    new.download_to_filename.assert_called_once()
    old.download_to_filename.assert_not_called()
    assert latest == new.time_created