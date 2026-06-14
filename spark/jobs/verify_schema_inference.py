"""
Compare schema inference for Parquet files with and without mergeSchema.

Usage:
  python spark/jobs/verify_schema_inference.py --paths /tmp/data/part-*.parquet
"""

import argparse
import glob
import traceback
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

    # Try reading with mergeSchema=true, but catch schema-merge errors and
    # fall back to printing per-file schemas so the user can see which files
    # disagree.
    try:
        df_merge = read_with_merge(spark, args.paths, merge=True)
        print_schema(df_merge, "Schema with mergeSchema=true")

        # Simple field-level comparison
        fields_nom = set([(f.name, f.dataType.simpleString()) for f in df_nomerge.schema.fields])
        fields_mer = set([(f.name, f.dataType.simpleString()) for f in df_merge.schema.fields])

        only_nom = fields_nom - fields_mer
        only_mer = fields_mer - fields_nom

        print("Fields only in no-merge:", only_nom)
        print("Fields only in merge:", only_mer)
    except Exception as exc:
        print("Error reading with mergeSchema=true:")
        traceback.print_exception(type(exc), exc, exc.__traceback__)
        print("\nFalling back to reading each file individually to show per-file schemas:\n")

        per_file_fields = {}
        # Expand globs and read each matching file
        for p in args.paths:
            matches = glob.glob(p)
            if not matches:
                matches = [p]
            for m in matches:
                try:
                    df = spark.read.parquet(m)
                    print_schema(df, f"Schema for {m}")
                    per_file_fields[m] = set((f.name, f.dataType.simpleString()) for f in df.schema.fields)
                except Exception as e2:
                    print(f"Failed reading {m}: {e2}")

        if per_file_fields:
            # aggregate fields inferred across files
            all_fields = set().union(*per_file_fields.values())
        else:
            all_fields = set()

        fields_nom = set([(f.name, f.dataType.simpleString()) for f in df_nomerge.schema.fields])
        fields_mer = all_fields

        only_nom = fields_nom - fields_mer
        only_mer = fields_mer - fields_nom

        print("Fields only in no-merge:", only_nom)
        print("Fields only across files (inferred merge):", only_mer)

    spark.stop()


if __name__ == "__main__":
    main()
