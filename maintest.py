"""
Blocks-DB End-to-End Test

This demonstrates the complete workflow:
1. Setup infrastructure
2. Upload data and index (auto-updates threshold)
3. Check status
4. Put new vectors (triggers auto-indexer Lambda)
5. Check status
6. Query
"""

import subprocess
import os
import sys
import time

BUCKET = os.environ.get("SVDB_BUCKET", "your-bucket")
REGION = os.environ.get("SVDB_REGION", "us-east-1")

DATASET = "test-dataset"
CSV_PATH = "test-dataset/vectors_test.csv"
PUT_CSV = "test-dataset/new_vectors.csv"
QUERY_FILE = "test-dataset/queries.csv"
CONFIG_PATH = "vectordb/config/indexconfig.json"

CLI = "blocks-db"


def run(cmd, check=True):
    """Run a CLI command."""
    full_cmd = f"{CLI} {cmd}"
    print(f"\n$ {full_cmd}")
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr and "warning" not in result.stderr.lower():
        print(result.stderr, file=sys.stderr)
    if check and result.returncode != 0:
        print(f"ERROR: Command failed with return code {result.returncode}")
        sys.exit(1)
    return result


def main():
    # ── Setup ─────────────────────────────────────────────
    print("\n" + "=" * 50)
    print("1. Setup infrastructure")
    print("=" * 50)
    run(f"setup --bucket {BUCKET}", check=False)

    # ── Configure ─────────────────────────────────────
    print("\n" + "=" * 50)
    print("2. Configure default bucket")
    print("=" * 50)
    run(f"configure --bucket {BUCKET} --region {REGION}")

    # ── Upload data and index ─────────────────────────────────
    print("\n" + "=" * 50)
    print("3. Upload data and index")
    print("=" * 50)
    run(f"initialize-database {DATASET} {CSV_PATH} --config {CONFIG_PATH} --workers 16")

    # ── Status ───────────────────────────────────────
    print("\n" + "=" * 50)
    print("4. Status (after initialize-database)")
    print("=" * 50)
    run(f"status {DATASET} -v")

    # ── Put new vectors ────────────────────────────────
    print("\n" + "=" * 50)
    print("5. Put new vectors (triggers auto-indexer)")
    print("=" * 50)
    run(f"put {DATASET} {PUT_CSV}")

    # Give Lambda time to process (in real scenario, this would be async)
    print("\nWaiting for auto-indexer Lambda...")
    time.sleep(5)

    # ── Status after put ─────────────────────────────
    print("\n" + "=" * 50)
    print("6. Status (after put)")
    print("=" * 50)
    run(f"status {DATASET} -v")

    # ── Query ───────────────────────────────────────
    print("\n" + "=" * 50)
    print("7. Query (hybrid - searches index + pending)")
    print("=" * 50)
    run(f"query {DATASET} --file {QUERY_FILE} --k 10")

    print("\n" + "=" * 50)
    print("DONE")
    print("=" * 50)


if __name__ == "__main__":
    main()