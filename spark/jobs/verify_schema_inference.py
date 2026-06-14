"""
Compare schema inference for Parquet files with and without mergeSchema.

Usage:
  python spark/jobs/verify_schema_inference.py --paths /tmp/data/part-*.parquet
"""

import argparse
from pyspark.sql import SparkSession


def build_spark(app_name="verify_schema_inference"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    return spark


def read_with_merge(spark, paths, merge=True):
    return spark.read.option("mergeSchema", str(merge).lower()).parquet(*paths)


def print_schema(df, title=None):
    if title:
        print(f"--- {title} ---")
    df.printSchema()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paths", nargs="+", required=True, help="Parquet files or globs")
    args = parser.parse_args()

    spark = build_spark()

    # Read with mergeSchema=false by default
    df_nomerge = read_with_merge(spark, args.paths, merge=False)
    print_schema(df_nomerge, "Schema with mergeSchema=false")

    df_merge = read_with_merge(spark, args.paths, merge=True)
    print_schema(df_merge, "Schema with mergeSchema=true")

    # Simple field-level comparison
    fields_nom = set([(f.name, f.dataType.simpleString()) for f in df_nomerge.schema.fields])
    fields_mer = set([(f.name, f.dataType.simpleString()) for f in df_merge.schema.fields])

    only_nom = fields_nom - fields_mer
    only_mer = fields_mer - fields_nom

    print("Fields only in no-merge:", only_nom)
    print("Fields only in merge:", only_mer)

    spark.stop()


if __name__ == "__main__":
    main()
