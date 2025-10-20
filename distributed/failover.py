import requests
import json
from datetime import datetime

NODES = [
    "http://127.0.0.1:5000/health",
    "http://127.0.0.1:5001/health",
    "http://127.0.0.1:5002/health"
]

LOG_FILE = "system_logs.json"

def write_log(event):
    log_entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "event": event
    }
    try:
        with open(LOG_FILE, "r") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []

    logs.insert(0, log_entry)
    with open(LOG_FILE, "w") as f:
        json.dump(logs[:100], f, indent=2)

def check_nodes():
    """Kiểm tra node còn sống"""
    alive = []
    for node in NODES:
        try:
            res = requests.get(node, timeout=3)
            if res.status_code == 200:
                alive.append(node)
                write_log(f"Node {node} hoạt động bình thường.")
            else:
                write_log(f"Node {node} phản hồi bất thường ({res.status_code}).")
        except requests.exceptions.RequestException:
            write_log(f"Node {node} không phản hồi (offline).")

    # Giả lập failover nếu Primary (port 5000) bị down
    if "http://127.0.0.1:5000/health" not in alive:
        write_log("Primary node bị lỗi — kích hoạt failover sang node khác.")
    return alive
