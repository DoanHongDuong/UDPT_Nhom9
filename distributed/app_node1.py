from flask import Flask, jsonify, request

app = Flask(__name__)
replica_data = {"users": []}

@app.route("/replica", methods=["POST"])
def receive_replica():
    item = request.get_json()
    replica_data["users"].append(item)
    return jsonify({"message": "Replica received"}), 200

@app.route("/data", methods=["GET"])
def get_data():
    return jsonify(replica_data)

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "OK"}), 200

if __name__ == "__main__":
    app.run(port=5001, debug=True)
