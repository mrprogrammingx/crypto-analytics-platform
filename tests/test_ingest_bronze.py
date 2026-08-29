"""Tests for the Spark Bronze ingestion job (``spark/jobs/ingest_bronze.py``).

PySpark needs a JVM, so the whole module is skipped when Java or PySpark is not
available — the same guard the other Spark tests use.
"""
import subprocess
import sys
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

try:
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

# Import the job under test only when Spark is importable so pytest can still
# collect (and then skip) this module in environments without PySpark.
if _pyspark_available:
    from spark.jobs.ingest_bronze import (
        build_spark,
        normalize_timestamp,
        read_inputs,
        standardize_columns,
        write_bronze,
    )


@pytest.fixture(scope="module")
def spark():
    session = build_spark(app_name="test_ingest_bronze", master="local[1]")
    yield session
    session.stop()


def _write_parquet(path, data):
    pq.write_table(pa.Table.from_pydict(data), str(path))


# --- normalize_timestamp ----------------------------------------------------


def test_normalize_timestamp_epoch_ms(spark, tmp_path):
    p = tmp_path / "epoch.parquet"
    _write_parquet(p, {"price": [100, 101], "timestamp": [1650000000000, 1650100000000]})

    df = read_inputs(spark, [str(p)], "parquet")
    assert dict(df.dtypes)["timestamp"] == "bigint"

    out = normalize_timestamp(df)
    assert dict(out.dtypes)["timestamp"] == "timestamp"

    dates = [r["d"] for r in out.selectExpr("date_format(timestamp, 'yyyy-MM-dd') as d").orderBy("d").collect()]
    assert dates == ["2022-04-15", "2022-04-16"]


def test_normalize_timestamp_iso_string(spark, tmp_path):
    p = tmp_path / "iso.parquet"
    _write_parquet(p, {"price": [200], "timestamp": ["2022-04-15T12:00:00Z"]})

    out = normalize_timestamp(read_inputs(spark, [str(p)], "parquet"))
    assert dict(out.dtypes)["timestamp"] == "timestamp"
    assert out.selectExpr("date_format(timestamp, 'yyyy-MM-dd') as d").first()["d"] == "2022-04-15"


def test_normalize_timestamp_already_timestamp_is_passthrough(spark):
    df = spark.createDataFrame([(1, datetime(2022, 4, 15, 12, 0, 0))], ["price", "timestamp"])
    assert normalize_timestamp(df) is df


def test_normalize_timestamp_missing_column_is_noop(spark):
    df = spark.createDataFrame([(1,), (2,)], ["price"])
    assert normalize_timestamp(df) is df
    assert normalize_timestamp(df, ts_col="event_time") is df


def test_normalize_timestamp_custom_column(spark, tmp_path):
    p = tmp_path / "custom.parquet"
    _write_parquet(p, {"price": [1], "event_time": [1650000000000]})

    out = normalize_timestamp(read_inputs(spark, [str(p)], "parquet"), ts_col="event_time")
    assert dict(out.dtypes)["event_time"] == "timestamp"


# --- read_inputs ----------------------------------------------------------


def test_read_inputs_parquet_multiple_paths(spark, tmp_path):
    p1 = tmp_path / "a.parquet"
    p2 = tmp_path / "b.parquet"
    _write_parquet(p1, {"price": [1.0]})
    _write_parquet(p2, {"price": [2.0]})

    df = read_inputs(spark, [str(p1), str(p2)], "parquet")
    assert df.count() == 2


def test_read_inputs_csv_infers_header_and_types(spark, tmp_path):
    csv = tmp_path / "in.csv"
    csv.write_text("symbol,price,quantity\nBTCUSDT,100.5,0.2\nBTCUSDT,101.0,0.3\n")

    df = read_inputs(spark, [str(csv)], "csv")
    assert df.count() == 2
    assert set(df.columns) == {"symbol", "price", "quantity"}
    assert dict(df.dtypes)["price"] == "double"


# --- standardize_columns ------------------------------------------------------


def test_standardize_columns_keeps_requested_subset(spark):
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert standardize_columns(df, ["a", "c"]).columns == ["a", "c"]


def test_standardize_columns_ignores_missing_requested_columns(spark):
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert standardize_columns(df, ["a", "missing"]).columns == ["a"]


def test_standardize_columns_without_keep_cols_returns_all(spark):
    df = spark.createDataFrame([(1, 2, 3)], ["a", "b", "c"])
    assert standardize_columns(df).columns == ["a", "b", "c"]
    assert standardize_columns(df, None).columns == ["a", "b", "c"]


# --- write_bronze -----------------------------------------------------------


def test_write_bronze_partitioned_roundtrip(spark, tmp_path):
    df = spark.createDataFrame(
        [("BTCUSDT", 100.0, "2022-04-15"), ("BTCUSDT", 101.0, "2022-04-16")],
        ["symbol", "price", "date"],
    )
    out = tmp_path / "bronze"
    write_bronze(df, str(out), partition_by="date")

    assert (out / "date=2022-04-15").is_dir()
    assert (out / "date=2022-04-16").is_dir()

    back = spark.read.parquet(str(out))
    assert back.count() == 2
    assert "date" in back.columns


def test_write_bronze_unpartitioned(spark, tmp_path):
    df = spark.createDataFrame([("BTCUSDT", 100.0)], ["symbol", "price"])
    out = tmp_path / "bronze_flat"
    write_bronze(df, str(out), partition_by=None)

    assert not [c for c in out.iterdir() if c.is_dir() and "=" in c.name]
    assert spark.read.parquet(str(out)).count() == 1


def test_write_bronze_overwrite_mode_replaces_data(spark, tmp_path):
    out = tmp_path / "bronze_ow"
    write_bronze(spark.createDataFrame([(1,)], ["price"]), str(out))
    write_bronze(spark.createDataFrame([(2,), (3,)], ["price"]), str(out))
    assert spark.read.parquet(str(out)).count() == 2


# --- main() end to end ----------------------------------------------------------


def test_main_end_to_end_derives_date_partition(tmp_path):
    raw = tmp_path / "raw.parquet"
    _write_parquet(
        raw,
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "price": [100.0, 101.0],
            "quantity": [0.1, 0.2],
            "timestamp": [1650000000000, 1650100000000],
        },
    )
    out = tmp_path / "bronze"

    ret = subprocess.run(
        [
            sys.executable,
            "spark/jobs/ingest_bronze.py",
            "--inputs", str(raw),
            "--output", str(out),
            "--format", "parquet",
            "--partition", "date",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert ret.returncode == 0, ret.stderr

    part_dirs = sorted(c.name for c in out.iterdir() if c.is_dir() and c.name.startswith("date="))
    assert part_dirs == ["date=2022-04-15", "date=2022-04-16"]
