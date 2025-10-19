import requests

# Danh sách node cần replicate (mô phỏng 3 node)
NODES = ["http://127.0.0.1:5001", "http://127.0.0.1:5002"]

def replicate_data(data):
    for node in NODES:
        try:
            requests.post(f"{node}/replica", json=data, timeout=2)
            print(f"[Replication] Sent data to {node}")
        except requests.exceptions.RequestException:
            print(f"[Replication] Failed to reach {node}")
