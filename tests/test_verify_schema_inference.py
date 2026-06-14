import subprocess
import pytest
import json
import os
from pathlib import Path

try:
    subprocess.run(["java", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    _java_available = True
except Exception:
    _java_available = False

try:
    import pyspark  # type: ignore
    _pyspark_available = True
except Exception:
    _pyspark_available = False

pytestmark = pytest.mark.skipif(not (_java_available and _pyspark_available), reason="Requires Java+PySpark")

from spark.jobs.verify_schema_inference import build_spark, read_with_merge
import pyarrow as pa
import pyarrow.parquet as pq


def write_parquet(path, table):
    pq.write_table(table, path)


def test_verify_schema_diff(tmp_path):
    # create two parquet files with same column but different types
    p1 = tmp_path / "part1.parquet"
    p2 = tmp_path / "part2.parquet"

    # first file: column 'a' as int32
    t1 = pa.table({"a": pa.array([1, 2], type=pa.int32())})
    write_parquet(str(p1), t1)

    # second file: column 'a' as string
    t2 = pa.table({"a": pa.array(["x", "y"], type=pa.string())})
    write_parquet(str(p2), t2)

    spark = build_spark(app_name="test_verify_schema")

    df_no_merge = read_with_merge(spark, [str(p1), str(p2)], merge=False)
    df_merge = read_with_merge(spark, [str(p1), str(p2)], merge=True)

    fields_nom = set([(f.name, f.dataType.simpleString()) for f in df_no_merge.schema.fields])
    fields_mer = set([(f.name, f.dataType.simpleString()) for f in df_merge.schema.fields])

    # There should be a difference between no-merge and merge schemas for column 'a'
    assert fields_nom != fields_mer

    spark.stop()
