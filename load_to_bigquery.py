import pandas as pd
from google.cloud import bigquery

# Load CSV
df = pd.read_csv("grafana_metrics.csv")

# Fix: Convert timestamp to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
print(df.dtypes)

# BigQuery config
project_id = "observability-459214"
dataset_id = "monitoring"
table_id = f"{project_id}.{dataset_id}.grafana_metrics"

client = bigquery.Client()

# Check if table exists
try:
    client.get_table(table_id)
    print(f"Table {table_id} exists. Loading data.")
except:
    print(f"Table not found. Creating table: {table_id}")
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("metric", "STRING"),
        bigquery.SchemaField("instance", "STRING"),
        bigquery.SchemaField("value", "FLOAT"),
        bigquery.SchemaField("cpu_p95", "FLOAT"),
        bigquery.SchemaField("mem_p95", "FLOAT"),
        bigquery.SchemaField("net_inp95", "FLOAT"),
        bigquery.SchemaField("net_outp95", "FLOAT"),
        bigquery.SchemaField("disk_io", "FLOAT"),
        bigquery.SchemaField("current_type", "STRING"),
        bigquery.SchemaField("instance_type", "STRING"),
        bigquery.SchemaField("vCPU", "INTEGER"),
        bigquery.SchemaField("memory_GB", "FLOAT"),
        bigquery.SchemaFiled("network_bandwidth", "INTEGER"),
        bigquery.SchemaField("catalog_disk_io", "INTEGER"),
        bigquery.SchemaField("recommendation", "STRING"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"Created table {table_id}")

# Load to BigQuery
job = client.load_table_from_dataframe(df, table_id)
job.result()
print("Data loaded into BigQuery")
