import json
import os
import socket
import time

WORKER_HOST = "0.0.0.0"
WORKER_PORT = 5000
MASTER_HOST = "master"
MASTER_PORT = 5000
WORKER_NAME = os.getenv("WORKER_NAME", "Worker")


def normalize_word(token):
    cleaned = []
    for ch in token.lower():
        if ("a" <= ch <= "z") or ("0" <= ch <= "9"):
            cleaned.append(ch)
    return "".join(cleaned)


def map_word_count(chunk_text):
    counts = {}
    lines = chunk_text.splitlines()
    print(f"{WORKER_NAME}: Received {len(lines)} lines")

    for line in lines:
        for token in line.split():
            word = normalize_word(token)
            if not word:
                continue
            counts[word] = counts.get(word, 0) + 1

    print(f"{WORKER_NAME}: Map complete with {len(counts)} unique words")
    return counts


def send_result_to_master(word_count):
    message = {
        "type": "MAP_RESULT",
        "worker": WORKER_NAME,
        "word_count": word_count,
    }
    payload = json.dumps(message).encode("utf-8")

    for attempt in range(1, 31):
        try:
            print(f"{WORKER_NAME}: Sending result to master:5000 (attempt {attempt})")
            with socket.create_connection((MASTER_HOST, MASTER_PORT), timeout=5) as s:
                s.sendall(payload)
            print(f"{WORKER_NAME}: Sending result complete")
            return True
        except Exception as e:
            print(f"{WORKER_NAME}: Failed to send result yet: {e}")
            time.sleep(1)

    print(f"{WORKER_NAME}: Failed to send result after retries")
    return False


def process_message(raw_bytes):
    try:
        message = json.loads(raw_bytes.decode("utf-8"))
    except Exception as e:
        print(f"{WORKER_NAME}: Invalid JSON payload: {e}")
        return

    msg_type = message.get("type")
    if msg_type != "MAP_TASK":
        print(f"{WORKER_NAME}: Unsupported message type: {msg_type}")
        return

    chunk = message.get("chunk", "")
    result = map_word_count(chunk)
    send_result_to_master(result)


def start_worker_server():
    print(f"{WORKER_NAME}: Starting server on {WORKER_HOST}:{WORKER_PORT}")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((WORKER_HOST, WORKER_PORT))
        server.listen(5)

        while True:
            print(f"{WORKER_NAME}: Waiting for map task...")
            conn, addr = server.accept()
            with conn:
                print(f"{WORKER_NAME}: Connected by {addr}")
                chunks = []
                while True:
                    packet = conn.recv(4096)
                    if not packet:
                        break
                    chunks.append(packet)

            raw = b"".join(chunks)
            if not raw:
                print(f"{WORKER_NAME}: Received empty payload")
                continue

            process_message(raw)


def main():
    print(f"{WORKER_NAME}: Booting up...")
    time.sleep(1)
    start_worker_server()


if __name__ == "__main__":
    main()
