from google.cloud import bigquery
import pandas as pd

# Load the DataFrame
df = pd.read_csv("grafana_metrics.csv")

# Set your project and dataset/table IDs
project_id = "observability-459214"
dataset_id = "monitoring"
table_id = f"{project_id}.{dataset_id}.grafana_metrics"

# Initialize client
client = bigquery.Client()

# Define schema (based on your CSV structure)
# 👉 Adjust these types to match your CSV columns
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("metric_name", "STRING"),
    bigquery.SchemaField("value", "FLOAT"),
]

# Check if table exists
try:
    client.get_table(table_id)
    print(f"✅ Table {table_id} exists. Loading data.")
except Exception as e:
    print(f"⚠️ Table not found. Creating table: {table_id}")
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)
    print(f"✅ Created table {table_id}")

# Load data
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for job to complete

print("✅ Data loaded successfully into BigQuery.")
