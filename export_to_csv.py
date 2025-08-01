import requests
import pandas as pd
import os
from datetime import datetime, timedelta

# Environment setup
PROM_URL = os.environ.get("PROMETHEUS_URL", "http://34.100.247.250:9090")
STEP = os.environ.get("STEP", "60")

# Time window for metrics (last 90 minutes)
now = datetime.utcnow()
start_time = int((now - timedelta(minutes=90)).timestamp())
end_time = int(now.timestamp())

# Prometheus metric queries
queries = {
    "cpu_p95": 'quantile_over_time(0.95, rate(node_cpu_seconds_total{mode!="idle"}[5m])[5m:])',
    "mem_p95": 'quantile_over_time(0.95, node_memory_Active_bytes[5m])',
    "net_in_p95": 'quantile_over_time(0.95, rate(node_network_receive_bytes_total[5m])[5m:])',
    "net_out_p95": 'quantile_over_time(0.95, rate(node_network_transmit_bytes_total[5m])[5m:])',
    "disk_io": 'rate(node_disk_io_time_seconds_total[5m])'
}

all_data = []

# Fetch and parse each metric
for metric_name, query in queries.items():
    response = requests.get(f"{PROM_URL}/api/v1/query_range", params={
        "query": query,
        "start": start_time,
        "end": end_time,
        "step": STEP
    })
    results = response.json().get("data", {}).get("result", [])
    for result in results:
        instance = result.get("metric", {}).get("instance", "unknown")
        for value in result["values"]:
            timestamp = datetime.utcfromtimestamp(float(value[0])).isoformat()
            all_data.append([timestamp, instance, metric_name, value[1]])

# Create long format DataFrame
df = pd.DataFrame(all_data, columns=["timestamp", "instance_id", "metric_name", "value"])
df["value"] = pd.to_numeric(df["value"], errors="coerce")
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Pivot metrics into columns
df_wide = df.pivot_table(
    index=["timestamp", "instance_id"],
    columns="metric_name",
    values="value",
    aggfunc="mean"
).reset_index()

# Add static system fields
df_wide["current_type"] = "on-demand"
df_wide["instance_type"] = "n1-standard-1"
df_wide["vCPU"] = 1
df_wide["memory_GB"] = 3.75
df_wide["network_bandwidth"] = "1 Gbps"
df_wide["catalog_disk_io"] = 100

# ✅ Advanced Recommendation Logic
df_wide["mem_gb"] = df_wide["mem_p95"] / (1024 ** 3)

def generate_recommendation(row):
    cpu = row["cpu_p95"]
    mem = row["mem_gb"]
    disk = row["disk_io"]

    if cpu > 0.08 or mem > 2 or disk > 0.002:
        return "scale-up"
    elif cpu < 0.02 and mem < 1 and disk < 0.001:
        return "scale-down"
    else:
        return "none"

df_wide["recommendation"] = df_wide.apply(generate_recommendation, axis=1)
df_wide["status_recommendation"] = "auto-generated"

# Drop temp column
df_wide.drop(columns=["mem_gb"], inplace=True)

# Reorder final columns
final_columns = [
    "timestamp", "instance_id", "cpu_p95", "mem_p95", "net_in_p95", "net_out_p95",
    "disk_io", "current_type", "instance_type", "vCPU", "memory_GB",
    "network_bandwidth", "catalog_disk_io", "recommendation", "status_recommendation"
]
df_wide = df_wide[final_columns]

# Save to timestamped CSV
filename = f"grafana_metrics_{now.strftime('%Y%m%d_%H%M%S')}.csv"
df_wide.to_csv(filename, index=False)
print(f"✅ CSV saved: {filename}")

# Save filename for Jenkins stage
with open("csv_name.txt", "w") as f:
    f.write(filename)
