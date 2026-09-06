#!/usr/bin/env python3
"""Create/replace a BigQuery external table over the Spark Bronze GCS output."""
import sys
from google.cloud import bigquery
from config import load_config


def main():
    cfg = load_config()
    if not (cfg.GOOGLE_CLOUD_PROJECT and cfg.BIGQUERY_TABLE_ID and cfg.GCS_BUCKET_NAME):
        print("GOOGLE_CLOUD_PROJECT, BIGQUERY_TABLE_ID and GCS_BUCKET_NAME must be set.")
        sys.exit(1)

    client = bigquery.Client(project=cfg.GOOGLE_CLOUD_PROJECT)
    parts = cfg.BIGQUERY_TABLE_ID.split(".")
    full_table_id = cfg.BIGQUERY_TABLE_ID if len(parts) == 3 else f"{cfg.GOOGLE_CLOUD_PROJECT}.{cfg.BIGQUERY_TABLE_ID}"
    source_prefix = f"gs://{cfg.GCS_BUCKET_NAME}/{cfg.SPARK_BRONZE_GCS_PREFIX}"

    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"{source_prefix}/*"]
    hive = bigquery.HivePartitioningOptions()
    hive.mode = "AUTO"
    hive.source_uri_prefix = source_prefix
    external_config.hive_partitioning = hive

    table = bigquery.Table(full_table_id)
    table.external_data_configuration = external_config

    client.delete_table(full_table_id, not_found_ok=True)
    client.create_table(table)
    print(f"Created external table {full_table_id} over {source_prefix}/*")


if __name__ == "__main__":
    main()