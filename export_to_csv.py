import requests
import pandas as pd
from datetime import datetime, timedelta

PROM_URL = "http://34.100.247.250:9090"
STEP = "60"
now = datetime.utcnow()
start_time = int((now - timedelta(minutes=5)).timestamp())
end_time = int(now.timestamp())

queries = {
    "cpu_usage": 'rate(node_cpu_seconds_total{mode!="idle"}[5m])',
    "memory_usage": 'node_memory_Active_bytes',
    "disk_available": 'node_filesystem_avail_bytes',
    "network_rx": 'rate(node_network_receive_bytes_total[5m])'
}

all_data = []

for metric_name, query in queries.items():
    response = requests.get(f"{PROM_URL}/api/v1/query_range", params={
        "query": query,
        "start": start_time,
        "end": end_time,
        "step": STEP
    })
    results = response.json()["data"]["result"]
    for result in results:
        for value in result["values"]:
            timestamp = datetime.utcfromtimestamp(float(value[0])).isoformat()
            all_data.append([timestamp, metric_name, value[1]])

df = pd.DataFrame(all_data, columns=["timestamp", "metric_name", "value"])
df.to_csv("grafana_metrics.csv", index=False)
print("CSV saved: grafana_metrics.csv")
