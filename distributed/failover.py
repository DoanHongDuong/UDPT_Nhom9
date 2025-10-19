import requests
import time

# Danh sách các node trong hệ thống
NODES = [
    "http://127.0.0.1:5000",
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002"
]

# Node chính ban đầu
current_master = "http://127.0.0.1:5000"


# =========================
# Kiểm tra node đang sống
# =========================
def check_nodes():
    alive = []
    for node in NODES:
        try:
            res = requests.get(f"{node}/health", timeout=2)
            if res.status_code == 200:
                alive.append(node)
        except:
            pass
    return alive


# =========================
# Giám sát failover
# =========================
def failover_monitor():
    global current_master
    while True:
        alive = check_nodes()
        print("Alive nodes:", alive)
        print("Current master:", current_master)

        if current_master not in alive:
            print(f"⚠️ {current_master} is DOWN! Initiating failover...")
            if alive:
                current_master = alive[0]
                print(f"✅ Failover complete — New master: {current_master}")
            else:
                print("🚨 No nodes alive! System down!")

        time.sleep(5)

if __name__ == "__main__":
    failover_monitor()
