import json
import socket
import sys
import threading
import time
from datetime import datetime

HOST = "127.0.0.1"
PORT_START = 5000
NODES = list(range(1, 6))
PING_INTERVAL = 3
TIMEOUT = 2

node_id = 0
leader_id = None
leader_lock = threading.Lock()
election_lock = threading.Lock()
leader_event = threading.Event()


def log(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] Node {node_id}: {message}", flush=True)


def send_message(target, message):
    try:
        with socket.create_connection(
            (HOST, PORT_START + target), timeout=TIMEOUT
        ) as sock:
            sock.settimeout(TIMEOUT)
            sock.sendall((json.dumps(message) + "\n").encode("utf-8"))
            with sock.makefile("r", encoding="utf-8") as stream:
                reply = stream.readline()

        return json.loads(reply) if reply else None
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def announce_leader():
    global leader_id

    with leader_lock:
        leader_id = node_id
    leader_event.set()
    log("I am the leader")

    for other in NODES:
        if other != node_id:
            send_message(other, {"type": "COORDINATOR", "sender": node_id})


def start_election():
    if not election_lock.acquire(blocking=False):
        return

    try:
        while True:
            log("Starting election")
            leader_event.clear()
            received_ok = False

            for other in NODES:
                if other <= node_id:
                    continue

                reply = send_message(
                    other, {"type": "ELECTION", "sender": node_id}
                )
                if reply and reply.get("type") == "OK":
                    received_ok = True
                    log(f"Node {other} replied OK")

            if not received_ok:
                announce_leader()
                return

            log("Waiting for a coordinator message")
            leader_event.wait(6)

            with leader_lock:
                current_leader = leader_id

            if current_leader is not None and current_leader > node_id:
                return

            log("No leader announcement received; starting again")
    finally:
        election_lock.release()


def receive_message(sock):
    with sock.makefile("r", encoding="utf-8") as stream:
        line = stream.readline()
    return json.loads(line) if line else None


def handle_client(sock):
    start_new_election = False

    with sock:
        try:
            sock.settimeout(TIMEOUT)
            message = receive_message(sock)
            if not message:
                return

            kind = message.get("type")
            sender = int(message.get("sender", 0))
            log(f"Received {kind} from Node {sender}")

            if kind == "ELECTION":
                reply = {"type": "OK", "sender": node_id}
                start_new_election = True

            elif kind == "COORDINATOR":
                global leader_id
                with leader_lock:
                    if sender > node_id and (
                        leader_id is None or sender >= leader_id
                    ):
                        leader_id = sender
                        leader_event.set()
                        log(f"Leader is Node {sender}")
                reply = {"type": "ACK", "sender": node_id}

            elif kind == "PING":
                reply = {"type": "PONG", "sender": node_id}

            else:
                reply = {"type": "ACK", "sender": node_id}

            sock.sendall((json.dumps(reply) + "\n").encode("utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            log("Could not process the message")

    if start_new_election:
        threading.Thread(target=start_election, daemon=True).start()


def listen_for_nodes():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT_START + node_id))
        server.listen()
        log(f"Listening on port {PORT_START + node_id}")

        while True:
            client, _ = server.accept()
            threading.Thread(
                target=handle_client, args=(client,), daemon=True
            ).start()


def monitor_leader():
    global leader_id

    while True:
        time.sleep(PING_INTERVAL)
        with leader_lock:
            current_leader = leader_id

        if current_leader is None or current_leader == node_id:
            continue

        reply = send_message(
            current_leader, {"type": "PING", "sender": node_id}
        )
        if reply and reply.get("type") == "PONG":
            continue

        log(f"Node {current_leader} is not responding")
        with leader_lock:
            if leader_id == current_leader:
                leader_id = None
        start_election()


def main():
    global node_id

    if len(sys.argv) != 2:
        print("Usage: python3 node.py <node_id>")
        raise SystemExit(1)

    try:
        node_id = int(sys.argv[1])
    except ValueError:
        print("Please enter a node ID from 1 to 5.")
        raise SystemExit(1)

    if node_id not in NODES:
        print("Please enter a node ID from 1 to 5.")
        raise SystemExit(1)

    log("Starting")
    threading.Thread(target=listen_for_nodes, daemon=True).start()
    time.sleep(0.5)
    threading.Thread(target=monitor_leader, daemon=True).start()
    start_election()
    threading.Event().wait()


if __name__ == "__main__":
    main()
