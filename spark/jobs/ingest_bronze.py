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

def read_inputs(spark, paths, fmt):
    if fmt == "parquet":
        return spark.read.option("mergeSchema", "true").parquet(*paths)
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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inputs", nargs="+", required=True, help="Input paths (file/dir/glob; support gs://)")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default="parquet", help="Input format")
    parser.add_argument("--output", required=True, help="Output path for bronze parquet")
    parser.add_argument("--timestamp-col", default="timestamp", help="Timestamp column to normalize")
    parser.add_argument("--partition", default="date", help="Partition column (default: date from timestamp)")
    parser.add_argument("--keep-cols", nargs="+", help="Optional list of columns to keep")
    parser.add_argument("--master", default="local[*]", help="Spark master (override for cluster)")
    parser.add_argument("--show", type=int, default=0, help="Show sample rows")
    args = parser.parse_args()

    spark = build_spark(master=args.master)
    df = read_inputs(spark, args.inputs, args.format)
    df = normalize_timestamp(df, ts_col=args.timestamp_col)
    # create a date partition column if timestamp available
    if args.timestamp_col in df.columns:
        df = df.withColumn(args.partition, date_format(col(args.timestamp_col), "yyyy-MM-dd"))
    df = standardize_columns(df, keep_cols=args.keep_cols)
    if args.show:
        df.show(args.show, truncate=False)
    write_bronze(df, args.output, partition_by=(args.partition if args.partition in df.columns else None))
    spark.stop()

if __name__ == "__main__":
    main()