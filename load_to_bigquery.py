from google.cloud import bigquery
import pandas as pd

# Authenticate using service account JSON
client = bigquery.Client.from_service_account_json("service-account.json")

# Load CSV
df = pd.read_csv("grafana_metrics.csv")

# Define your table destination: project_id.dataset.table_name
table_id = "observability-459214.monitoring.grafana_metrics"

# Load to BigQuery
job = client.load_table_from_dataframe(df, table_id)
job.result()  # Wait for the job to complete

print("✅ Data uploaded to BigQuery")
