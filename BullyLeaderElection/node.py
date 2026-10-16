import json
import socket
import sys
import threading
import time
from datetime import datetime

HOST = "127.0.0.1"
BASE_PORT = 5000
NODES = [1, 2, 3, 4, 5]

PING_INTERVAL = 3
SOCKET_TIMEOUT = 2
COORDINATOR_TIMEOUT = 6

node_id = 0
leader_id = None

state_lock = threading.Lock()
election_lock = threading.Lock()
coordinator_event = threading.Event()


def log(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] Node {node_id}: {message}", flush=True)


def send_message(target, message):
    """Send one JSON message and read the reply."""
    try:
        with socket.create_connection(
            (HOST, BASE_PORT + target), timeout=SOCKET_TIMEOUT
        ) as sock:
            sock.settimeout(SOCKET_TIMEOUT)
            sock.sendall((json.dumps(message) + "\n").encode("utf-8"))

            with sock.makefile("r", encoding="utf-8") as stream:
                reply_line = stream.readline()

        if not reply_line:
            return None
        return json.loads(reply_line)

    except (OSError, ValueError, json.JSONDecodeError):
        return None


def announce_leader():
    """Tell every other node that this node is the leader."""
    global leader_id

    with state_lock:
        leader_id = node_id
    coordinator_event.set()
    log("I am the new leader")

    for target in NODES:
        if target != node_id:
            send_message(
                target, {"type": "COORDINATOR", "sender": node_id}
            )


def start_election():
    """Run the Bully election process for this node."""
    global leader_id

    if not election_lock.acquire(blocking=False):
        return

    try:
        while True:
            log("Starting Bully election")
            coordinator_event.clear()

            higher_nodes = [n for n in NODES if n > node_id]
            got_ok = False

            for target in higher_nodes:
                reply = send_message(
                    target, {"type": "ELECTION", "sender": node_id}
                )
                if reply and reply.get("type") == "OK":
                    got_ok = True
                    log(f"Node {target} replied OK")

            if not got_ok:
                announce_leader()
                return

            log("Waiting for a coordinator message")
            coordinator_event.wait(COORDINATOR_TIMEOUT)

            with state_lock:
                current_leader = leader_id

            if current_leader is not None and current_leader > node_id:
                return

            log("No higher coordinator received; retrying election")

    finally:
        election_lock.release()


def read_message(conn):
    with conn.makefile("r", encoding="utf-8") as stream:
        line = stream.readline()
    return json.loads(line) if line else None


def handle_message(conn):
    global leader_id
    start_another_election = False

    with conn:
        try:
            conn.settimeout(SOCKET_TIMEOUT)
            message = read_message(conn)

            if not message:
                return

            kind = message.get("type")
            sender = int(message.get("sender", 0))
            log(f"Received {kind} from Node {sender}")

            if kind == "ELECTION":
                reply = {"type": "OK", "sender": node_id}
                start_another_election = True

            elif kind == "COORDINATOR":
                if sender > node_id:
                    with state_lock:
                        if leader_id is None or sender >= leader_id:
                            leader_id = sender
                            coordinator_event.set()
                            log(f"Leader announced: Node {sender}")
                reply = {"type": "ACK", "sender": node_id}

            elif kind == "PING":
                reply = {"type": "PONG", "sender": node_id}

            else:
                reply = {"type": "ACK", "sender": node_id}

            conn.sendall((json.dumps(reply) + "\n").encode("utf-8"))

        except (OSError, ValueError, json.JSONDecodeError):
            log("Could not process the message")
            return

    if start_another_election:
        threading.Thread(target=start_election, daemon=True).start()


def listen_for_nodes():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, BASE_PORT + node_id))
        server.listen()
        log(f"Listening on port {BASE_PORT + node_id}")

        while True:
            conn, _ = server.accept()
            threading.Thread(
                target=handle_message, args=(conn,), daemon=True
            ).start()


def monitor_leader():
    global leader_id

    while True:
        time.sleep(PING_INTERVAL)

        with state_lock:
            current_leader = leader_id

        if current_leader is None or current_leader == node_id:
            continue

        reply = send_message(
            current_leader, {"type": "PING", "sender": node_id}
        )
        if reply and reply.get("type") == "PONG":
            continue

        log(f"Leader Node {current_leader} did not respond")

        with state_lock:
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
        print("Node ID must be an integer from 1 to 5.")
        raise SystemExit(1)

    if node_id not in NODES:
        print("Node ID must be between 1 and 5.")
        raise SystemExit(1)

    log("Starting")
    threading.Thread(target=listen_for_nodes, daemon=True).start()
    time.sleep(0.5)
    threading.Thread(target=monitor_leader, daemon=True).start()

    start_election()
    threading.Event().wait()


if __name__ == "__main__":
    main()
