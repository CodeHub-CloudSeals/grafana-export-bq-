import os
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

df = pd.read_csv("grafana_metrics.csv")
table_id = "observability-459214.monitoring.grafana_metrics"

job = client.load_table_from_dataframe(df, table_id)
job.result()

print("✅ Data uploaded to BigQuery")
