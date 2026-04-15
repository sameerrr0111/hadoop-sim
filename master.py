import json
import os
import socket
import time

MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000
EXPECTED_WORKERS = ["worker1", "worker2"]
DATA_FILE = "/app/data.csv"
OUTPUT_FILE = "/app/output.txt"
WORD_BANK = [
    "hadoop", "map", "reduce", "data", "big", "python", "cluster", "node", "job", "task",
    "network", "socket", "master", "worker", "count", "word", "file", "split", "shuffle", "result",
    "analytics", "batch", "processing", "storage", "system", "compute", "distributed", "parallel", "input", "output",
]


def generate_data_file_if_missing(path, line_count=10000):
    if os.path.exists(path) and os.path.getsize(path) > 0:
        print(f"Master: Using existing {path}")
        return

    print(f"Master: {path} not found or empty. Generating {line_count} lines...")
    with open(path, "w", encoding="utf-8") as f:
        for _ in range(line_count):
            words_in_line = 8 + (os.urandom(1)[0] % 7)
            words = []
            for _ in range(words_in_line):
                idx = os.urandom(1)[0] % len(WORD_BANK)
                words.append(WORD_BANK[idx])
            f.write(" ".join(words) + "\n")
    print(f"Master: Generated {line_count} lines in {path}")


def read_and_split_data(path):
    print("Master: Reading data.csv...")
    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total_lines = len(lines)
    split_index = total_lines // 2

    print(f"Master: Splitting file... total_lines={total_lines}, split_index={split_index}")
    chunk1 = "".join(lines[:split_index])
    chunk2 = "".join(lines[split_index:])

    print(f"Master: Chunk1 lines={len(lines[:split_index])}")
    print(f"Master: Chunk2 lines={len(lines[split_index:])}")
    return chunk1, chunk2


def send_map_task(worker_host, chunk):
    message = {
        "type": "MAP_TASK",
        "chunk": chunk,
    }
    payload = json.dumps(message).encode("utf-8")

    for attempt in range(1, 31):
        try:
            print(f"Master: Sending chunk to {worker_host}:5000 (attempt {attempt})")
            with socket.create_connection((worker_host, 5000), timeout=5) as s:
                s.sendall(payload)
            print(f"Master: Successfully sent chunk to {worker_host}")
            return True
        except Exception as e:
            print(f"Master: Could not send to {worker_host} yet: {e}")
            time.sleep(1)

    print(f"Master: Failed to send chunk to {worker_host} after retries")
    return False


def receive_map_results(expected_count=2):
    print(f"Master: Starting reduce listener on {MASTER_HOST}:{MASTER_PORT}")
    results = []

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((MASTER_HOST, MASTER_PORT))
        server.listen(expected_count)

        while len(results) < expected_count:
            print(f"Master: Waiting for map result {len(results) + 1}/{expected_count}...")
            conn, addr = server.accept()
            with conn:
                print(f"Master: Connection from {addr}")
                chunks = []
                while True:
                    packet = conn.recv(4096)
                    if not packet:
                        break
                    chunks.append(packet)

                raw = b"".join(chunks)
                if not raw:
                    print("Master: Received empty response")
                    continue

                try:
                    message = json.loads(raw.decode("utf-8"))
                    worker_name = message.get("worker", "UnknownWorker")
                    word_count = message.get("word_count", {})
                    print(f"Master: Received result from {worker_name} with {len(word_count)} unique words")
                    results.append(word_count)
                except Exception as e:
                    print(f"Master: Failed to decode worker response: {e}")

    print("Master: All map results received")
    return results


def reduce_word_counts(partials):
    print("Master: Starting reduce phase...")
    final_counts = {}
    for partial in partials:
        for word, count in partial.items():
            final_counts[word] = final_counts.get(word, 0) + int(count)
    print("Master: Reduce complete")
    return final_counts


def write_output(path, final_counts):
    print(f"Master: Writing full output to {path}")
    sorted_items = sorted(final_counts.items(), key=lambda item: (-item[1], item[0]))
    with open(path, "w", encoding="utf-8") as f:
        for word, count in sorted_items:
            f.write(f"{word},{count}\n")


def print_top_10(final_counts):
    print("FINAL REDUCE OUTPUT:")
    sorted_items = sorted(final_counts.items(), key=lambda item: (-item[1], item[0]))
    for idx, (word, count) in enumerate(sorted_items[:10], start=1):
        print(f"{idx}. {word}: {count}")


def main():
    print("Master: Booting up...")
    time.sleep(2)

    generate_data_file_if_missing(DATA_FILE, line_count=10000)
    chunk1, chunk2 = read_and_split_data(DATA_FILE)

    sent1 = send_map_task("worker1", chunk1)
    sent2 = send_map_task("worker2", chunk2)

    if not sent1 or not sent2:
        print("Master: Could not send all tasks. Exiting.")
        return

    partial_results = receive_map_results(expected_count=len(EXPECTED_WORKERS))
    if len(partial_results) != len(EXPECTED_WORKERS):
        print("Master: Missing map results. Exiting.")
        return

    final_counts = reduce_word_counts(partial_results)
    print_top_10(final_counts)
    write_output(OUTPUT_FILE, final_counts)
    print("Master: Job finished successfully")


if __name__ == "__main__":
    main()
