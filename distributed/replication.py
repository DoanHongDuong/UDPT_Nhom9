import requests
import json
from datetime import datetime

# Danh sách node phụ (thay URL tùy theo project bạn)
NODES = [
    "http://127.0.0.1:5001/replica",
    "http://127.0.0.1:5002/replica"
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
        json.dump(logs[:100], f, indent=2)  # giữ 100 log gần nhất

def replicate_data(user):
    """Gửi dữ liệu người dùng sang các node khác"""
    for node in NODES:
        try:
            res = requests.post(node, json=user, timeout=3)
            if res.status_code == 200:
                write_log(f"Replication thành công tới {node}")
            else:
                write_log(f"Replication lỗi {res.status_code} tới {node}")
        except requests.exceptions.RequestException:
            write_log(f"Không thể kết nối tới {node}")
