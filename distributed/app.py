import sys
import threading
import time
import requests
import json
import os
from tinydb import TinyDB, Query  
from flask import Flask, request, jsonify, render_template, redirect, url_for
from datetime import datetime

# Danh sách TẤT CẢ các node trong hệ thống
ALL_NODE_URLS = [
    "http://127.0.0.1:5000",
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002"
]
HEARTBEAT_INTERVAL = 5  # Giây

MY_PORT = 5000
MY_URL = "http://127.0.0.1:5000"
DATA_FILE = "users_5000.json"
LOG_FILE = "logs_5000.json"
db = None  

CURRENT_ROLE = "REPLICA"  
CURRENT_PRIMARY_URL = None
KNOWN_REPLICAS = []
LAST_HEARTBEAT_OK = True

# --- Khởi tạo Flask ---
app = Flask(__name__, template_folder="templates", static_folder="static")


def load_data():
    """Đọc tất cả user từ TinyDB.""" 
    if db is None:
        return {"users": []} 
    return {"users": db.all()}


def save_data(data):
    """
    Hàm này bây giờ CHỈ dùng để ĐỒNG BỘ TOÀN BỘ.
    Nó sẽ xóa sạch DB cũ và chèn dữ liệu mới.
    """ 
    if db is None: return

    users_list = data.get("users", [])
    db.truncate()  
    db.insert_multiple(users_list)  


def write_log(event):
    log_entry = {
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "role": CURRENT_ROLE,
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


def replicate_data(user_data):
    """Chỉ Primary mới gọi hàm này. Gửi dữ liệu tới tất cả Replica đã biết."""
    write_log(f"Bắt đầu sao chép dữ liệu tới {len(KNOWN_REPLICAS)} replicas.")
    for replica_url in KNOWN_REPLICAS:
        try:
            res = requests.post(f"{replica_url}/replica", json=user_data, timeout=2)
            if res.status_code == 200:
                write_log(f"Sao chép thành công tới {replica_url}")
            else:
                write_log(f"Lỗi sao chép tới {replica_url}: {res.status_code}")
        except requests.exceptions.RequestException:
            write_log(f"Không thể kết nối tới replica {replica_url} để sao chép.")


def check_nodes_and_update_role():
    global CURRENT_ROLE, CURRENT_PRIMARY_URL, KNOWN_REPLICAS, LAST_HEARTBEAT_OK

    alive_nodes = []
    for node_url in ALL_NODE_URLS:
        try:
            res = requests.get(f"{node_url}/health", timeout=1)
            if res.status_code == 200:
                alive_nodes.append(node_url)
        except requests.exceptions.RequestException:
            pass  # Cứ bỏ qua node chết

    if not alive_nodes:
        CURRENT_ROLE = "OFFLINE"
        CURRENT_PRIMARY_URL = None
        KNOWN_REPLICAS = []
        LAST_HEARTBEAT_OK = False
        write_log("Không tìm thấy node nào còn sống.")
        return

    new_primary = None

    # 1. Kiểm tra xem Primary hiện tại (nếu có) có còn sống không
    if CURRENT_PRIMARY_URL and CURRENT_PRIMARY_URL in alive_nodes:
        new_primary = CURRENT_PRIMARY_URL
    else:
        # Primary cũ đã chết HOẶC chưa có Primary -> Bầu chọn cái mới
        new_primary = min(alive_nodes) 
        if CURRENT_PRIMARY_URL:
            write_log(f"Primary cũ {CURRENT_PRIMARY_URL} đã offline. Bầu chọn Primary mới: {new_primary}")
        else:
            write_log(f"Chưa có Primary. Bầu chọn Primary mới: {new_primary}")

    # 2. Cập nhật trạng thái
    CURRENT_PRIMARY_URL = new_primary

    if MY_URL == new_primary:
        if CURRENT_ROLE != "PRIMARY":
            write_log("THĂNG CẤP: Node này trở thành PRIMARY.")
        CURRENT_ROLE = "PRIMARY"
        KNOWN_REPLICAS = [url for url in alive_nodes if url != MY_URL]
    else:
        if CURRENT_ROLE != "REPLICA":
            write_log(f"GIÁNG CẤP: Node này là REPLICA của {new_primary}.")
        CURRENT_ROLE = "REPLICA"
        KNOWN_REPLICAS = []

    LAST_HEARTBEAT_OK = True


def background_heartbeat():
    """Chạy hàm check_nodes liên tục trong một thread riêng."""
    while True:
        check_nodes_and_update_role()
        time.sleep(HEARTBEAT_INTERVAL)


def sync_with_cluster():
    """
    Hàm này được gọi DUY NHẤT MỘT LẦN khi khởi động.
    Nó tìm Primary hiện tại và sao chép toàn bộ dữ liệu.
    """
    global CURRENT_ROLE, CURRENT_PRIMARY_URL

    write_log("Bắt đầu khởi động, tìm Primary để đồng bộ...")

    other_nodes = [url for url in ALL_NODE_URLS if url != MY_URL]
    found_primary = None

    # 1. Tìm xem ai là Primary
    for node_url in other_nodes:
        try:
            res = requests.get(f"{node_url}/api/system_status", timeout=2)
            if res.status_code == 200:
                status = res.json()
                if status.get("current_primary"):
                    found_primary = status["current_primary"]
                    write_log(f"Node {node_url} báo Primary là: {found_primary}")
                    break  # Tìm thấy là đủ
        except requests.exceptions.RequestException:
            write_log(f"Không thể kết nối tới {node_url} khi tìm Primary.")
            pass  # Node đó có thể đang offline

    # 2. Đồng bộ dữ liệu
    if found_primary:
        try:
            write_log(f"Đang tải dữ liệu từ Primary {found_primary}...")
            res = requests.get(f"{found_primary}/api/full_data", timeout=5)
            if res.status_code == 200:
                full_data = res.json()
                save_data(full_data)  
                CURRENT_PRIMARY_URL = found_primary
                CURRENT_ROLE = "REPLICA"  
                write_log(f"Đồng bộ thành công! Đã tải {len(full_data.get('users', []))} users.")
            else:
                write_log(f"Lỗi khi tải data từ Primary {found_primary}. Status: {res.status_code}")
        except requests.exceptions.RequestException:
            write_log(f"Không thể kết nối tới Primary {found_primary} để đồng bộ.")
    else:
        # Không tìm thấy Primary nào khác
        write_log("Không tìm thấy Primary nào. Coi mình là node đầu tiên.")
        CURRENT_PRIMARY_URL = None
        CURRENT_ROLE = "REPLICA"


@app.route("/")
def home():
    return jsonify({
        "message": "Distributed System API is running",
        "my_url": MY_URL,
        "role": CURRENT_ROLE,
        "current_primary": CURRENT_PRIMARY_URL
    })


@app.route("/dashboard")
def dashboard():
    """Bất kỳ node nào cũng có thể phục vụ dashboard."""
    return render_template("dashboard.html")


@app.route("/health", methods=["GET"])
def health_check():
    """Endpoint để các node khác kiểm tra xem node này còn sống không."""
    return jsonify({"status": "OK"}), 200


@app.route("/replica", methods=["POST"])
def receive_replica():
    """Endpoint chỉ REPLICA mới dùng, để nhận dữ liệu từ PRIMARY."""
    if CURRENT_ROLE != "REPLICA":
        return jsonify({"error": "Tôi là Primary, không nhận replica"}), 400

    user = request.get_json()

    # --- ĐÃ SỬA ---
    if db is None:
        return jsonify({"error": "Database not initialized"}), 500
    db.insert(user)  

    write_log(f"Nhận và lưu trữ dữ liệu sao chép cho: {user.get('name')}")
    return jsonify({"message": "Replica received"}), 200


@app.route("/api/add_user", methods=["POST"])
def add_user():
    """
    Route 'thông minh':
    - Nếu là PRIMARY: Tự xử lý.
    - Nếu là REPLICA: Chuyển tiếp (forward) request sang PRIMARY.
    """
    if CURRENT_ROLE == "PRIMARY":
        # 1. Logic của Primary: Tự xử lý
        user = request.get_json()
        if not user or "name" not in user:
            write_log("Dữ liệu người dùng không hợp lệ khi thêm.")
            return jsonify({"error": "Invalid user data"}), 400

        if db is None:
            return jsonify({"error": "Database not initialized"}), 500
        db.insert(user)  

        write_log(f"PRIMARY: Đã thêm người dùng: {user.get('name')}")

        # Bắt đầu sao chép sang các replica
        replicate_data(user)

        return jsonify({"message": "User added by Primary and replicated!", "data": user}), 201

    elif CURRENT_ROLE == "REPLICA":
        # 2. Logic của Replica: Chuyển tiếp
        if not CURRENT_PRIMARY_URL:
            return jsonify({"error": "Hệ thống đang bầu chọn, vui lòng thử lại sau"}), 503

        write_log(f"REPLICA: Chuyển tiếp /api/add_user tới {CURRENT_PRIMARY_URL}")
        try:
            # Chuyển tiếp request y hệt sang Primary
            res = requests.post(
                f"{CURRENT_PRIMARY_URL}/api/add_user",
                json=request.get_json(),
                timeout=3
            )
            # Trả về kết quả từ Primary cho người dùng
            return jsonify(res.json()), res.status_code
        except requests.exceptions.RequestException:
            return jsonify({"error": "Không thể kết nối tới Primary"}), 504
    else:
        return jsonify({"error": "Node đang offline"}), 503


@app.route("/api/users")
def get_users():
    """Bất kỳ node nào cũng có thể trả về dữ liệu nó đang có."""
    return jsonify(load_data()) # <-- Tự động dùng TinyDB


@app.route("/api/logs")
def get_logs():
    """Bất kỳ node nào cũng trả về log của chính nó."""
    try:
        with open(LOG_FILE, "r") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []
    return jsonify(logs)


@app.route("/api/nodes")
def get_nodes():
    """Trả về trạng thái các node mà node này 'biết'."""
    nodes = []
    alive_nodes = []
    for node_url in ALL_NODE_URLS:
        try:
            res = requests.get(f"{node_url}/health", timeout=1)
            if res.status_code == 200:
                alive_nodes.append(node_url)
        except requests.exceptions.RequestException:
            pass

    for url in ALL_NODE_URLS:
        is_alive = url in alive_nodes
        role = "OFFLINE"
        if url == CURRENT_PRIMARY_URL:
            role = "PRIMARY"
        elif is_alive:
            role = "REPLICA"

        nodes.append({"name": f"Node {url.split(':')[-1]}", "url": url, "alive": is_alive, "role": role})

    return jsonify(nodes)


@app.route("/api/system_status")
def get_system_status():
    """Trả về Primary và Role hiện tại của node này."""
    return jsonify({
        "role": CURRENT_ROLE,
        "current_primary": CURRENT_PRIMARY_URL,
        "my_url": MY_URL
    })


@app.route("/api/full_data")
def get_full_data():
    """Bất kỳ node nào cũng trả về dữ liệu nó đang có."""
    write_log(f"Cung cấp full data (với tư cách {CURRENT_ROLE}).")
    return jsonify(load_data()) 

# 5. KHỞI CHẠY ỨNG DỤNG

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Lỗi: Cần cung cấp port")
        print("Cách dùng: python app.py [port]")
        print("Ví dụ: python app.py 5000")
        sys.exit(1)

    try:
        MY_PORT = int(sys.argv[1])
        if f"http://127.0.0.1:{MY_PORT}" not in ALL_NODE_URLS:
            print(f"Lỗi: Port {MY_PORT} không nằm trong danh sách ALL_NODE_URLS.")
            sys.exit(1)

    except ValueError:
        print("Lỗi: Port phải là một con số.")
        sys.exit(1)

    # Cập nhật các biến toàn cục dựa trên port
    MY_URL = f"http://127.0.0.1:{MY_PORT}"
    DATA_FILE = f"users_{MY_PORT}.json"
    LOG_FILE = f"logs_{MY_PORT}.json"

    db = TinyDB(DATA_FILE)  # <-- ĐÃ THÊM: Khởi tạo DB

    print(f"--- Khởi chạy Node tại {MY_URL} ---")

    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)  # Xóa log cũ thì OK

    # Chạy đồng bộ TRƯỚC KHI khởi chạy heartbeat
    sync_with_cluster()

    # Khởi chạy thread nền để kiểm tra "sức khỏe"
    heartbeat_thread = threading.Thread(target=background_heartbeat, daemon=True)
    heartbeat_thread.start()

    # Chạy Flask app
    app.run(host='0.0.0.0', port=MY_PORT, debug=True, use_reloader=False)