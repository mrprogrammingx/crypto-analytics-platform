import subprocess
import pytest

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
    # two parquet files: same column name, incompatible types
    p1 = tmp_path / "part1.parquet"
    p2 = tmp_path / "part2.parquet"

    write_parquet(str(p1), pa.table({"a": pa.array([1, 2], type=pa.int32())}))
    write_parquet(str(p2), pa.table({"a": pa.array(["x", "y"], type=pa.string())}))

    spark = build_spark(app_name="test_verify_schema")
    try:
        # Without mergeSchema, Spark infers from a single file's footer and succeeds.
        df_no_merge = read_with_merge(spark, [str(p1), str(p2)], merge=False)
        assert dict(df_no_merge.dtypes)["a"] in ("int", "string")

        # With mergeSchema, Spark reconciles footers and fails hard on incompatible
        # types (INT vs STRING). verify_schema_inference.main() catches this and
        # falls back to printing per-file schemas.
        with pytest.raises(Exception) as excinfo:
            read_with_merge(spark, [str(p1), str(p2)], merge=True)
        assert "incompatible data types" in str(excinfo.value).lower()
    finally:
        spark.stop()
