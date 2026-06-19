"""
Blocks-DB End-to-End Test
==========================
Tests: index → query → put (tagged) → query hybrid → put (untagged) → auto-index → query filtered → delete-dataset

Usage:
  python maintest.py

Override paths via env vars or edit variables below.
"""

import subprocess
import os
import sys
import time

# ── Config ───────────────────────────────────────────────────
BUCKET = os.environ.get("SVDB_BUCKET", "yourbucket")
REGION = os.environ.get("SVDB_REGION", "us-east-1")

DATASET = "maintest-database"
CSV_PATH = "vectors_deep_100k.csv"
TAGGED_PUT = "tags_secondput.csv"
UNTAGGED_PUT = "secondput.csv"
QUERY_FILE = "test_queries.csv"
CONFIG_PATH = "vectordb/config/indexconfig.json"

CLI = "blocks-db"


def run(cmd, check=True):
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


def section(n, title):
    print("\n" + "=" * 50)
    print(f"{n}. {title}")
    print("=" * 50)


def main():
    # ── 0. Refresh credentials ────────────────────────────────
    section(0, "Refresh AWS credentials")
    run("refresh-credentials", check=False)

    # ── 1. Setup ──────────────────────────────────────────────
    section(1, "Setup infrastructure")
    run(f"setup --bucket {BUCKET}", check=False)

    # ── 2. Configure ──────────────────────────────────────────
    section(2, "Configure default bucket")
    run(f"configure --bucket {BUCKET} --region {REGION}")

    # ── 3. Initialize database ────────────────────────────────
    section(3, "Initialize database from deep_100k")
    run(f"initialize-database {DATASET} {CSV_PATH} --config {CONFIG_PATH} --workers 16")

    # ── 4. Query (indexed only) ───────────────────────────────
    section(4, "Query (indexed only — no pending yet)")
    run(f"query {DATASET} --file {QUERY_FILE} --k 10")

    # ── 5. Status ─────────────────────────────────────────────
    section(5, "Status (after initialize-database)")
    run(f"status {DATASET} -v")

    # ── 6. Put tagged vectors ─────────────────────────────────
    section(6, "Put tagged vectors (pending)")
    run(f"put {DATASET} {TAGGED_PUT}")

    # ── 7. Status ─────────────────────────────────────────────
    section(7, "Status (after first put — should show pending)")
    run(f"status {DATASET} -v")

    # ── 8. Query hybrid ───────────────────────────────────────
    section(8, "Query hybrid (indexed + pending tagged)")
    run(f"query {DATASET} --file {QUERY_FILE} --k 10")

    # ── 9. Put untagged vectors ───────────────────────────────
    section(9, "Put untagged vectors (pending)")
    run(f"put {DATASET} {UNTAGGED_PUT}")

    # ── 10. Wait for auto-indexer ─────────────────────────────
    section(10, "Wait for auto-indexer Lambda")
    print("\nWaiting 2s...")
    time.sleep(2)

    # ── 11. Status ────────────────────────────────────────────
    section(11, "Status (should be autoindexed — no pending)")
    run(f"status {DATASET} -v")

    # ── 12. Query with filter tags ────────────────────────────
    section(12, "Query with tag filter (pre-mode — fewer workers)")
    run(f'query {DATASET} --file {QUERY_FILE} --k 10 --filter \'{{"source":"api"}}\' --filter-mode pre')

    # ── 13. Delete dataset ────────────────────────────────────
    section(13, "Delete dataset")
    run(f"delete-dataset {DATASET} --yes")

    print("\n" + "=" * 50)
    print("ALL DONE")
    print("=" * 50)


if __name__ == "__main__":
    main()
