import pandas as pd
from google.cloud import bigquery

# Load CSV
df = pd.read_csv("grafana_metrics.csv")
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

# Config
project_id = "observability-459214"
dataset_id = "monitoring"
table_id = f"{project_id}.{dataset_id}.grafana_metrics"

client = bigquery.Client()

# Explicit schema
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
]

# Check and create table if needed
try:
    client.get_table(table_id)
    print(f"Table {table_id} exists.")
except:
    print(f"Table not found. Creating {table_id} ...")
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"Created table: {table.table_id}")

# Load data with matching schema (no autodetect)
job = client.load_table_from_dataframe(df, table_id)
job.result()

print("Data loaded successfully into BigQuery.")
