
import os
import pyarrow as pa
import pyarrow.parquet as pq
import subprocess
import pytest

try:
    # check java availability (Spark needs JVM)
    subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    _java_available = True
except Exception:
    _java_available = False

try:
    from pyspark.sql import SparkSession  # type: ignore
    _pyspark_available = True
except Exception:
    _pyspark_available = False

skip_reason = "Skipping Spark tests: Java JVM or PySpark not available"
pytestmark = pytest.mark.skipif(not (_java_available and _pyspark_available), reason=skip_reason)

from spark.jobs.read_btc_parquet import build_spark, normalize_timestamp, read_parquet


def create_parquet_from_dict(path, data_dict):
    # Build a pyarrow table directly from a dict of lists to avoid pandas
    table = pa.Table.from_pydict(data_dict)
    pq.write_table(table, path)


def test_read_parquet_and_normalize_epoch(tmp_path):
    spark = build_spark(app_name="test_read_parquet_epoch")

    # create data with epoch ms
    data = {
        "price": [100, 101],
        "timestamp": [1650000000000, 1650000001000],  # ms
    }

    p = tmp_path / "epoch.parquet"
    create_parquet_from_dict(str(p), data)

    sdf = read_parquet(spark, str(p))
    assert "timestamp" in sdf.columns

    sdf2 = normalize_timestamp(sdf)
    # timestamp column should now be of timestamp type
    dtype = dict(sdf2.dtypes)["timestamp"]
    assert dtype in ("timestamp", "TimestampType") or "timestamp" in dtype.lower()

    spark.stop()


def test_read_parquet_and_normalize_iso(tmp_path):
    spark = build_spark(app_name="test_read_parquet_iso")

    data = {
        "price": [200],
        "timestamp": ["2022-04-15T12:00:00Z"],
    }

    p = tmp_path / "iso.parquet"
    create_parquet_from_dict(str(p), data)

    sdf = read_parquet(spark, str(p))
    sdf2 = normalize_timestamp(sdf)

    dtype = dict(sdf2.dtypes)["timestamp"]
    assert dtype in ("timestamp", "TimestampType") or "timestamp" in dtype.lower()

    spark.stop()
