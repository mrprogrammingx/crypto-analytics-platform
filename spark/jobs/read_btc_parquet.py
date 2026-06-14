"""
Simple PySpark job to read BTC Parquet files (local or GCS), normalize timestamp
and print basic diagnostics. Designed for local/dev use (master=local[*]).

Usage examples:
  # read local parquet dir
  python spark/jobs/read_btc_parquet.py --path /tmp/btc_trades

  # read a single parquet file
  python spark/jobs/read_btc_parquet.py --path /tmp/btc_trades/part-000.parquet

  # read from GCS (runner must have GCS connector / credentials available)
  python spark/jobs/read_btc_parquet.py --path gs://my-bucket/btc_trades

Notes:
- If reading from GCS, run on an environment with the GCS Hadoop connector (Dataproc or a machine with the connector jars).
- Alternatively, use `gsutil` or `gcsfs` to download files locally before running this script.
- This script attempts to coerce a `timestamp` column into Spark TimestampType. If your data stores epoch milliseconds, it will convert accordingly.
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import TimestampType


def build_spark(app_name="read_btc_parquet"):
    # local[*] is convenient for dev; CI or cluster runs should override via SPARK_MASTER or cluster submission
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def normalize_timestamp(df):
    # If there's a timestamp column, try to coerce to TimestampType.
    if "timestamp" not in df.columns:
        return df

    ttype = [t for c, t in df.dtypes if c == "timestamp"]
    if not ttype:
        return df
    ttype = ttype[0]

    # bigint/long stored as epoch milliseconds
    if ttype in ("bigint", "long", "int"):
        # convert ms -> seconds then to timestamp
        return df.withColumn("timestamp", to_timestamp((col("timestamp") / 1000)))

    # string; try to parse ISO or unix-style
    if ttype == "string":
        # to_timestamp will try multiple formats; if your strings are iso-8601 this should work
        return df.withColumn("timestamp", to_timestamp(col("timestamp")))

    # already a timestamp
    if ttype == "timestamp":
        return df

    # fallback: attempt coercion
    try:
        return df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    except Exception:
        return df


def read_parquet(spark, path):
    # use mergeSchema option in case partitions have schema differences
    return spark.read.option("mergeSchema", "true").parquet(path)


def main():
    parser = argparse.ArgumentParser(description="Read BTC Parquet files with PySpark")
    parser.add_argument("--path", required=True, help="Path to parquet files (dir, file, or gs://...)")
    parser.add_argument("--show", type=int, default=10, help="Number of sample rows to show")
    parser.add_argument("--count", action="store_true", help="Print row count")
    parser.add_argument("--save", help="Optional path to write a normalized Parquet output")
    args = parser.parse_args()

    path = args.path

    spark = build_spark()

    if path.startswith("gs://"):
        print("Reading from GCS. Ensure this runner has the GCS connector (or run on Dataproc). If not, download files locally first.")
        # If you want to set credential file for the GCS Hadoop connector, uncomment and set below.
        # keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        # if keyfile:
        #     hadoop_conf = spark._jsc.hadoopConfiguration()
        #     hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
        #     hadoop_conf.set("google.cloud.auth.service.account.json.keyfile", keyfile)

    print(f"Reading parquet path: {path}")
    df = read_parquet(spark, path)

    print("Raw schema:")
    df.printSchema()

    df = normalize_timestamp(df)

    print("Post-normalize schema:")
    df.printSchema()

    if args.count:
        print("Row count:", df.count())

    n = args.show
    print(f"Showing {n} rows:")
    df.show(n, truncate=False)

    if args.save:
        print(f"Writing normalized parquet to: {args.save}")
        df.write.mode("overwrite").parquet(args.save)

    spark.stop()


if __name__ == "__main__":
    main()
