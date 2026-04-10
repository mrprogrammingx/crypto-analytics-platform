#!/usr/bin/env python3
"""Create BigQuery dataset and table if they don't exist.

Reads configuration from `config.load_config()` (so .env is respected).

Usage:
  ./scripts/create_bigquery_table.py

This will create the dataset (if missing) and the table `btc_trades` with a
simple schema: symbol STRING, price FLOAT64, quantity FLOAT64, timestamp TIMESTAMP.
The table will be partitioned by day on the `timestamp` column to support efficient queries and ingestion from Parquet.
"""
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from config import load_config
import sys
import argparse


def parse_args():
    p = argparse.ArgumentParser(description="Create BigQuery dataset and table if missing")
    p.add_argument(
        "--recreate",
        action="store_true",
        help="Delete the table if it exists and recreate it",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="When used with --recreate, skip interactive confirmation",
    )
    p.add_argument(
        "--tracking",
        action="store_true",
        help="Also create a tracking table named `loaded_files` in the same dataset",
    )
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_config()
    # Prefer GOOGLE_CLOUD_PROJECT 
    project = cfg.GOOGLE_CLOUD_PROJECT
    # prefer explicit BIGQUERY_TABLE_ID, otherwise use dataset + centralized table name
    table_id = cfg.BIGQUERY_TABLE_ID
    dataset_name = cfg.BIGQUERY_DATASET
    table_name = cfg.BIGQUERY_TABLE_BTC_TRADES
    tracking_table_name = cfg.BIGQUERY_TRACKING_TABLE
    if not project:
        print("GOOGLE_CLOUD_PROJECT is not set in the environment (.env). Aborting.")
        sys.exit(1)

    if not table_id:
        print("BIGQUERY_TABLE_ID is not set. Aborting.")
        sys.exit(1)

    # If BIGQUERY_TABLE_ID is provided use it; otherwise use central dataset/table values
    if table_id:
        parts = table_id.split('.')
        if len(parts) == 3:
            project_from_table, dataset_id, table_name = parts
        elif len(parts) == 2:
            project_from_table = project
            dataset_id, table_name = parts
        else:
            print("BIGQUERY_TABLE_ID must be in the form dataset.table or project.dataset.table")
            sys.exit(1)
    else:
        dataset_id = dataset_name
        table_name = table_name

    client = bigquery.Client(project=project)

    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {project}:{dataset_id} already exists")
    except NotFound:
        print(f"Creating dataset {project}:{dataset_id}...")
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        print("Created dataset:", dataset.full_dataset_id)

    table_ref = dataset_ref.table(table_name)
    try:
        client.get_table(table_ref)
        table_exists = True
        print(f"Table {project}.{dataset_id}.{table_name} already exists")
    except NotFound:
        table_exists = False

    if table_exists and args.recreate:
        if not args.force:
            resp = input(
                f"Are you sure you want to delete table {project}.{dataset_id}.{table_name}? [y/N]: "
            ).strip().lower()
            if resp not in ("y", "yes"):
                print("Aborting table recreation")
                sys.exit(0)
        print(f"Deleting table {project}.{dataset_id}.{table_name}...")
        client.delete_table(table_ref, not_found_ok=True)
        table_exists = False

    if not table_exists:
        print(f"Creating table {project}.{dataset_id}.{table_name}...")
        schema = [
            bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("price", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("quantity", "FLOAT64", mode="NULLABLE"),
            # use TIMESTAMP so Parquet -> BigQuery load jobs infer correctly
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        # Partition by day on the timestamp column
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp",
        )
        table = client.create_table(table)
        print("Created table:", table.full_table_id)

    # Optionally create a tracking table for loaded files
    if args.tracking:
        tracking_table_ref = dataset_ref.table(tracking_table_name or "loaded_files")
        try:
            client.get_table(tracking_table_ref)
            print(f"Tracking table {project}.{dataset_id}.loaded_files already exists")
        except NotFound:
            print(f"Creating tracking table {project}.{dataset_id}.loaded_files...")
            tracking_schema = [
                bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE"),
            ]
            tracking_table = bigquery.Table(tracking_table_ref, schema=tracking_schema)
            client.create_table(tracking_table)
            print(f"Created tracking table: {project}.{dataset_id}.loaded_files")


if __name__ == "__main__":
    main()
