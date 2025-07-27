import pandas as pd
from google.cloud import bigquery

# Load CSV
df = pd.read_csv("grafana_metrics.csv")

# ✅ Fix: Convert timestamp column to datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

# BigQuery config
project_id = "observability-459214"
dataset_id = "monitoring"
table_id = f"{project_id}.{dataset_id}.grafana_metrics"

client = bigquery.Client()

# If table doesn't exist, create it
try:
    client.get_table(table_id)
    print("⚠️ Table exists.")
except:
    print(f"⚠️ Table not found. Creating table: {table_id}")
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("metric", "STRING"),
        bigquery.SchemaField("instance", "STRING"),
        bigquery.SchemaField("value", "FLOAT"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    print(f"✅ Created table {table_id}")

# Load to BigQuery
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete
print("✅ Data loaded into BigQuery")
