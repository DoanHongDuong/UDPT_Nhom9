from flask import Flask, request, jsonify, render_template
import json, os
from replication import replicate_data, write_log
from failover import check_nodes

app = Flask(__name__, template_folder="templates", static_folder="static")

DATA_FILE = "users.json"
LOG_FILE = "system_logs.json"

def load_data():
    if not os.path.exists(DATA_FILE):
        return {"users": []}
    with open(DATA_FILE, "r") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {"users": []}

def save_data(data):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

@app.route("/")
def home():
    return jsonify({"message": "Distributed System API is running "})

@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")

@app.route("/api/users")
def get_users():
    return jsonify(load_data())

@app.route("/api/add_user", methods=["POST"])
def add_user():
    user = request.get_json()
    if not user or "name" not in user:
        write_log("Dữ liệu người dùng không hợp lệ khi thêm.")
        return jsonify({"error": "Invalid user data"}), 400

    data = load_data()
    data["users"].append(user)
    save_data(data)

    replicate_data(user)
    write_log(f"Đã thêm người dùng mới: {user.get('name')} ({user.get('email')})")
    return jsonify({"message": "User added and replicated!", "data": user}), 201

@app.route("/api/nodes")
def get_nodes():
    alive = check_nodes()
    nodes = [
        {"name": "Primary", "url": "http://127.0.0.1:5000", "alive": "http://127.0.0.1:5000/health" in alive},
        {"name": "Node1", "url": "http://127.0.0.1:5001", "alive": "http://127.0.0.1:5001/health" in alive},
        {"name": "Node2", "url": "http://127.0.0.1:5002", "alive": "http://127.0.0.1:5002/health" in alive},
    ]
    return jsonify(nodes)

@app.route("/api/logs")
def get_logs():
    try:
        with open(LOG_FILE, "r") as f:
            logs = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []
    return jsonify(logs)

if __name__ == "__main__":
    app.run(debug=True)
