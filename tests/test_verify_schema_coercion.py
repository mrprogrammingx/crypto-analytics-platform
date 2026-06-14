import subprocess
import pytest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

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


def write_parquet(path, table):
    pq.write_table(table, path)


def test_fallback_and_coercion(tmp_path, capsys):
    p1 = tmp_path / "part1.parquet"
    p2 = tmp_path / "part2.parquet"

    t1 = pa.table({"a": pa.array([1, 2], type=pa.int32())})
    t2 = pa.table({"a": pa.array(["x", "y"], type=pa.string())})

    write_parquet(str(p1), t1)
    write_parquet(str(p2), t2)

    # Run the script without coercion: should fallback and print per-file schemas
    ret = subprocess.run(["python", "spark/jobs/verify_schema_inference.py", "--paths", str(p1), str(p2)], check=False, capture_output=True, text=True)
    assert "Falling back to reading each file individually" in ret.stdout
    assert "Schema for" in ret.stdout

    # Run the script with coercion to string: should print coerced schema
    ret2 = subprocess.run(["python", "spark/jobs/verify_schema_inference.py", "--paths", str(p1), str(p2), "--coerce-to", "string"], check=False, capture_output=True, text=True)
    assert "Attempting to coerce conflicting columns to 'string'" in ret2.stdout
    assert "Schema after coercion to string" in ret2.stdout
