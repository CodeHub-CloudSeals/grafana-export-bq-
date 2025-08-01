import pandas as pd
import os
from google.cloud import bigquery

# Read filename from export script
with open("csv_name.txt") as f:
    csv_file = f.read().strip()

# Load CSV
df = pd.read_csv(csv_file)

# Drop nulls to avoid BQ upload issues
df.dropna(subset=["timestamp", "instance_id"], inplace=True)

# Convert timestamp
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# Ensure all columns exist
if "status_recommendation" not in df.columns:
    df["status_recommendation"] = None

# BigQuery Config
project_id = "observability-459214"
dataset_id = "monitoring"
table_id = f"{project_id}.{dataset_id}.grafana_metrics"

client = bigquery.Client()

# Define schema
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("instance_id", "STRING"),
    bigquery.SchemaField("cpu_p95", "FLOAT"),
    bigquery.SchemaField("mem_p95", "FLOAT"),
    bigquery.SchemaField("net_in_p95", "FLOAT"),
    bigquery.SchemaField("net_out_p95", "FLOAT"),
    bigquery.SchemaField("disk_io", "FLOAT"),
    bigquery.SchemaField("current_type", "STRING"),
    bigquery.SchemaField("instance_type", "STRING"),
    bigquery.SchemaField("vCPU", "INTEGER"),
    bigquery.SchemaField("memory_GB", "FLOAT"),
    bigquery.SchemaField("network_bandwidth", "STRING"),
    bigquery.SchemaField("catalog_disk_io", "INTEGER"),
    bigquery.SchemaField("recommendation", "STRING"),
    bigquery.SchemaField("status_recommendation", "STRING"),
]

# Create table if not exists
try:
    client.get_table(table_id)
    print(f"✅ Table {table_id} exists.")
except:
    print(f"⚠️ Table not found. Creating {table_id} ...")
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"✅ Created table: {table.table_id}")

# Load data
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    schema_update_options=["ALLOW_FIELD_ADDITION"]
)

job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print(f"✅ Uploaded {len(df)} rows to {table_id}")
