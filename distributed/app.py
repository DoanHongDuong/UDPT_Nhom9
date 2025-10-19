from flask import Flask, request, jsonify
import json, os
from replication import replicate_data
from failover import check_nodes

app = Flask(__name__)

DATA_FILE = "users.json"

# Load va save data
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

data_store = load_data()

# Cac route

@app.route("/")
def home():
    return jsonify({"message": "Distributed System API is running 🚀"})

# Them user moi va replicate
@app.route("/add_user", methods=["POST"])
def add_user():
    user = request.get_json()
    if not user or "name" not in user:
        return jsonify({"error": "Invalid user data"}), 400

    data_store["users"].append(user)
    save_data(data_store)

    # Gửi bản sao sang node khác
    replicate_data(user)
    return jsonify({"message": "User added and replicated!", "data": user}), 201

# Nhận dữ liệu replicate từ node khác
@app.route("/replica", methods=["POST"])
def replica():
    user = request.get_json()
    if not user:
        return jsonify({"error": "No data received"}), 400

    data_store["users"].append(user)
    save_data(data_store)
    return jsonify({"message": "Replication received", "data": user}), 200

# Endpoint kiểm tra sức khỏe node
@app.route("/health")
def health():
    return jsonify({"status": "OK"}), 200

# Kiểm tra node sống (failover)
@app.route("/check_nodes")
def check_nodes_status():
    alive = check_nodes()
    return jsonify({"alive_nodes": alive}), 200

# Main
if __name__ == "__main__":
    app.run(debug=True)
