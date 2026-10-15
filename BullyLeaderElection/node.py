
import socket
import threading
import json
import sys
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
    """Send one newline-delimited JSON message and read its reply."""
    try:
        with socket.create_connection(
            (HOST, BASE_PORT + target),
            timeout=SOCKET_TIMEOUT
        ) as sock:
            sock.settimeout(SOCKET_TIMEOUT)

            outgoing = json.dumps(message) + "\n"
            sock.sendall(outgoing.encode("utf-8"))

            with sock.makefile("r", encoding="utf-8") as stream:
                line = stream.readline()

            if not line:
                return None

            return json.loads(line)

    except (OSError, ValueError, json.JSONDecodeError):
        return None


def handle_message(conn):
    global leader_id
    trigger_election = False

    with conn:
        try:
            conn.settimeout(SOCKET_TIMEOUT)

            with conn.makefile("r", encoding="utf-8") as stream:
                line = stream.readline()

            if not line:
                return

            message = json.loads(line)
            kind = message.get("type")
            sender = int(message.get("sender", 0))

            log(f"Received {kind} from Node {sender}")

            if kind == "ELECTION":
                reply = {"type": "OK", "sender": node_id}
                trigger_election = True

            elif kind == "COORDINATOR":
                with state_lock:
                    leader_id = sender
                coordinator_event.set()
                log(f"Leader announced: Node {sender}")
                reply = {"type": "ACK", "sender": node_id}

            elif kind == "PING":
                reply = {"type": "PONG", "sender": node_id}

            else:
                reply = {"type": "ACK", "sender": node_id}

            conn.sendall(
                (json.dumps(reply) + "\n").encode("utf-8")
            )

        except (OSError, ValueError, json.JSONDecodeError):
            log("Ignored an invalid or incomplete message")
            return

    # Send the reply before starting another election.
    if trigger_election:
        threading.Thread(target=start_election, daemon=True).start()


def server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((HOST, BASE_PORT + node_id))
        sock.listen()

        log(f"Listening on port {BASE_PORT + node_id}")

        while True:
            conn, _ = sock.accept()
            threading.Thread(
                target=handle_message,
                args=(conn,),
                daemon=True
            ).start()


def announce_leader():
    global leader_id

    with state_lock:
        leader_id = node_id

    coordinator_event.set()
    log("I am the new leader")

    for target in NODES:
        if target != node_id:
            send_message(target, {
                "type": "COORDINATOR",
                "sender": node_id
            })


def start_election():
    global leader_id

    # Prevent overlapping elections within the same node.
    if not election_lock.acquire(blocking=False):
        return

    try:
        while True:
            log("Starting Bully election")

            with state_lock:
                leader_id = None

            coordinator_event.clear()
            higher_nodes = [n for n in NODES if n > node_id]
            higher_node_replied = False

            for target in higher_nodes:
                response = send_message(target, {
                    "type": "ELECTION",
                    "sender": node_id
                })

                if response and response.get("type") == "OK":
                    higher_node_replied = True
                    log(f"Node {target} is alive")

            if not higher_node_replied:
                announce_leader()
                return

            log("Waiting for a higher node to announce its leadership")

            if coordinator_event.wait(COORDINATOR_TIMEOUT):
                with state_lock:
                    current = leader_id

                if current is not None and current > node_id:
                    return

            log("No valid higher leader announced; retrying election")

    finally:
        election_lock.release()


def monitor_leader():
    global leader_id

    while True:
        time.sleep(PING_INTERVAL)

        with state_lock:
            current = leader_id

        if current is None or current == node_id:
            continue

        response = send_message(current, {
            "type": "PING",
            "sender": node_id
        })

        if response and response.get("type") == "PONG":
            continue

        log(f"Leader Node {current} did not respond")

        with state_lock:
            if leader_id == current:
                leader_id = None

        start_election()


def main():
    global node_id

    if len(sys.argv) != 2:
        print("Usage: python3 node.py <node_id>")
        sys.exit(1)

    try:
        node_id = int(sys.argv[1])
    except ValueError:
        print("Node ID must be an integer from 1 to 5.")
        sys.exit(1)

    if node_id not in NODES:
        print("Node ID must be between 1 and 5.")
        sys.exit(1)

    log("Starting")

    try:
        threading.Thread(target=server, daemon=True).start()
        time.sleep(0.5)

        threading.Thread(
            target=monitor_leader,
            daemon=True
        ).start()

        start_election()
        threading.Event().wait()

    except OSError as error:
        log(f"Startup error: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
s