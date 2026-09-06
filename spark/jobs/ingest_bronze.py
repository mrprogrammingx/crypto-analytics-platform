"""
Spark Bronze ingestion job

Reads raw crypto datasets (parquet/csv/glob/GCS), normalizes timestamps and columns,
and writes a Bronze parquet dataset (optionally partitioned).

Usage:
  python spark/jobs/ingest_bronze.py --inputs /path/to/raw/*.parquet --output /tmp/warehouse/bronze/btc --format parquet --partition date
"""
import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, date_format
from pyspark.sql.types import TimestampType

try:
    from spark.jobs.schema_utils import read_with_schema_reconciliation
    from spark.jobs import gcs_sync
except ImportError:
    from schema_utils import read_with_schema_reconciliation
    import gcs_sync

def build_spark(app_name="ingest_bronze", master="local[*]"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def normalize_timestamp(df, ts_col="timestamp"):
    if ts_col not in df.columns:
        return df
    ttype = [t for c, t in df.dtypes if c == ts_col]
    if not ttype:
        return df
    ttype = ttype[0]
    if ttype in ("bigint", "long", "int"):
        # assume epoch milliseconds
        return df.withColumn(ts_col, to_timestamp((col(ts_col) / 1000)))
    if ttype == "string":
        return df.withColumn(ts_col, to_timestamp(col(ts_col)))
    if ttype == "timestamp":
        return df
    try:
        return df.withColumn(ts_col, col(ts_col).cast(TimestampType()))
    except Exception:
        return df

def read_inputs(spark, paths, fmt, coerce_to=None, ts_col="timestamp"):
    if fmt == "parquet":
        return read_with_schema_reconciliation(
            spark,
            paths,
            coerce_to=coerce_to,
            normalize_fn=lambda df: normalize_timestamp(df, ts_col=ts_col),
        )
    if fmt == "csv":
        return spark.read.option("header", "true").option("inferSchema", "true").csv(*paths)
    # fallback: try spark autodetect by format
    return spark.read.format(fmt).load(*paths)

def standardize_columns(df, keep_cols=None):
    if keep_cols:
        existing = [c for c in keep_cols if c in df.columns]
        return df.select(*existing)
    return df

def write_bronze(df, out_path, partition_by=None, mode="overwrite"):
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(out_path)
    
def apply_quality_filters(df):
    for c in ("price", "quantity"):
        if c in df.columns:
            df = df.filter(col(c).isNotNull() & (col(c) > 0))
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", nargs="+", required=False, help="Input paths (file/dir/glob; support gs://)")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default="parquet", help="Input format")
    parser.add_argument("--output", required=False, help="Output path for bronze parquet")
    parser.add_argument("--timestamp-col", default="timestamp", help="Timestamp column to normalize")
    parser.add_argument("--partition", default="date", help="Partition column (default: date from timestamp)")
    parser.add_argument("--keep-cols", nargs="+", help="Optional list of columns to keep")
    parser.add_argument("--master", default="local[*]", help="Spark master (override for cluster)")
    parser.add_argument("--show", type=int, default=0, help="Show sample rows")
    parser.add_argument("--source", choices=["local", "gcs"], default="local")
    parser.add_argument("--gcs-bucket", default=None)
    parser.add_argument("--gcs-prefix", default=None)
    parser.add_argument("--gcs-dest-prefix", default=None)
    parser.add_argument("--coerce-to", choices=["string", "int", "float"])
    args = parser.parse_args()

    from config import load_config
    cfg = load_config()

    latest = None
    if args.source == "gcs":
        import tempfile
        bucket = args.gcs_bucket or cfg.GCS_BUCKET_NAME
        prefix = args.gcs_prefix or cfg.SPARK_RAW_GCS_PREFIX
        dest_prefix = args.gcs_dest_prefix or cfg.SPARK_BRONZE_GCS_PREFIX
        output = args.output or cfg.SPARK_BRONZE_LOCAL_PATH
        since = gcs_sync.read_watermark(bucket, project=cfg.GOOGLE_CLOUD_PROJECT)
        staging_dir = tempfile.mkdtemp(prefix="bronze_staging_")
        inputs, latest = gcs_sync.download_new_blobs(bucket, prefix, staging_dir, since=since, project=cfg.GOOGLE_CLOUD_PROJECT)
        if not inputs:
            print("No new raw files since last watermark; nothing to do.")
            return
    else:
        if not args.inputs:
            parser.error("--inputs is required when --source local")
        inputs, output = args.inputs, args.output
        if not output:
            parser.error("--output is required")
            
    spark = build_spark(master=args.master)
    df = read_inputs(spark, inputs, args.format, coerce_to=args.coerce_to, ts_col=args.timestamp_col)
    df = normalize_timestamp(df, ts_col=args.timestamp_col)
    # create a date partition column if timestamp available
    if args.timestamp_col in df.columns:
        df = df.withColumn(args.partition, date_format(col(args.timestamp_col), "yyyy-MM-dd"))
    df = standardize_columns(df, keep_cols=args.keep_cols)
    df = apply_quality_filters(df)
    dedup_cols = [c for c in ("symbol", args.timestamp_col) if c in df.columns]
    if dedup_cols:
        df = df.dropDuplicates(dedup_cols)
    if args.show:
        df.show(args.show, truncate=False)
    write_mode = "append" if args.source == "gcs" else "overwrite"
    write_bronze(df, output, partition_by=(args.partition if args.partition in df.columns else None), mode=write_mode)
    spark.stop()

    if args.source == "gcs":
        gcs_sync.upload_output(output, bucket, dest_prefix, project=cfg.GOOGLE_CLOUD_PROJECT)
        if latest:
            gcs_sync.write_watermark(bucket, latest, project=cfg.GOOGLE_CLOUD_PROJECT)

if __name__ == "__main__":
    main()