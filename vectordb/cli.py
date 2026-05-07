import argparse
import csv
import json
import os
from pathlib import Path

from .client import VectorDBClient
from .infra import run_setup, refresh_lithops_credentials, get_infra_config
from .config import DEFAULT_INFRA_CONFIG


CONFIG_DIR = Path.home() / ".blocks-db-config"
CONFIG_FILE = CONFIG_DIR / "backend_config.json"


def load_backend_config():
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {}


def main():
    parser = argparse.ArgumentParser(
        prog="blocks-db",
        description="Blocks-DB: Serverless Vector Database",
        formatter_class=lambda prog: argparse.RawDescriptionHelpFormatter(prog, max_help_position=40)
    )

    parser.add_argument("--bucket", help="S3 bucket")
    parser.add_argument("--region", help="AWS region")

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # ── setup ─────────────────────────────────────────────────
    setup_parser = subparsers.add_parser("setup", help="Setup infrastructure (Lambda, DynamoDB, S3 triggers)")
    setup_parser.add_argument("--bucket", required=True, help="S3 bucket for storage")
    setup_parser.add_argument("--runtime-name", default=None, help="Runtime image name in ECR")
    setup_parser.add_argument("--function-name", default=None, help="Lambda function name")
    setup_parser.add_argument("--table-name", default=None, help="DynamoDB table name")
    setup_parser.add_argument("--layer-name", default=None, help="Lambda layer name")
    setup_parser.add_argument("--role-name", default=None, help="Lambda execution role name")
    setup_parser.add_argument("--threshold", type=int, default=None, help="Auto-indexer threshold in bytes (default: 5242880)")
    setup_parser.add_argument("--skip-vector-table", action="store_true", help="Skip DynamoDB table creation")
    setup_parser.add_argument("--skip-runtime", action="store_true", help="Skip Lithops runtime build")

    # ── update-threshold ─────────────────────────────────────
    threshold_parser = subparsers.add_parser("update-threshold", help="Update auto-indexer block size threshold")
    threshold_parser.add_argument("threshold_bytes", nargs="?", type=int, default=None, help="Threshold in bytes (optional, auto-calculate if not provided)")
    threshold_parser.add_argument("--dataset", default=None, help="Dataset name (for auto-calculate)")
    threshold_parser.add_argument("--bucket", default=None, help="S3 bucket (required if --dataset used)")

    # ── deploy-lambda ───────────────────────────────────
    deploy_lambda_parser = subparsers.add_parser("deploy-lambda", help="Deploy/update Lambda code")
    deploy_lambda_parser.add_argument("--function", default=None, help="Lambda function name")

    subparsers.add_parser("refresh-credentials", help="Refresh AWS credentials")

    configure_parser = subparsers.add_parser("configure", help="Save default bucket and region")
    configure_parser.add_argument("--bucket", required=True, help="Default S3 bucket")
    configure_parser.add_argument("--region", default="us-east-1", help="AWS region")

    # ── initialize-database ───────────────────────────────────
    init_parser = subparsers.add_parser("initialize-database", help="Upload initial dataset and create index")
    init_parser.add_argument("name", help="Dataset name")
    init_parser.add_argument("csv_path", help="Path to CSV file with vectors")
    init_parser.add_argument("--config", required=True, help="Path to index config JSON")
    init_parser.add_argument("--workers", type=int, default=16, help="Number of indexing workers")
    init_parser.add_argument("--no-update-threshold", action="store_true", help="Skip auto-update threshold after indexing")

    # ── put ───────────────────────────────────────────────────
    put_parser = subparsers.add_parser("put", help="Add new vectors (stored as individual files)")
    put_parser.add_argument("name", help="Dataset name")
    put_parser.add_argument("csv_path", help="CSV file with vectors (id, vector values)")
    put_parser.add_argument("--single", action="store_true", help="Treat as single vector per file (one vector per file)")

    # ── query ─────────────────────────────────────────────────
    query_parser = subparsers.add_parser("query", help="Query vectors (searches indexed + pending)")
    query_parser.add_argument("name", help="Dataset name")
    query_group = query_parser.add_mutually_exclusive_group(required=True)
    query_group.add_argument("--vector", help="Space-separated float values")
    query_group.add_argument("--file", help="CSV file with query vectors")
    query_parser.add_argument("--k", type=int, default=10, help="Number of results")
    query_parser.add_argument("--indexed-only", action="store_true", help="Search only indexed vectors (skip pending)")

    # ── status ────────────────────────────────────────────────
    status_parser = subparsers.add_parser("status", help="Show dataset status")
    status_parser.add_argument("name", help="Dataset name")
    status_parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed info")

    # ── get ───────────────────────────────────────────────────
    get_parser = subparsers.add_parser("get", help="Get vectors by ID, list vectors, or show pending")
    get_parser.add_argument("name", help="Dataset name")
    get_parser.add_argument("ids", nargs="*", type=int, help="Vector IDs (optional)")
    get_parser.add_argument("--limit", type=int, default=None, help="Limit number of vectors to list")
    get_parser.add_argument("--pending", action="store_true", help="Get pending vectors instead of main")

    args = parser.parse_args()

    VISIBLE_COMMANDS = {
        "setup", "configure", "refresh-credentials", "update-threshold",
        "initialize-database", "put", "query", "status", "get",
        "deploy-lambda"
    }

    if not args.command:
        parser.print_help()
        return

    if args.command not in VISIBLE_COMMANDS:
        print(f"Command '{args.command}' is not available.")
        print("Run 'blocks-db --help' to see available commands.")
        return

    file_config = load_backend_config()

    bucket = args.bucket or os.getenv("SVDB_BUCKET") or file_config.get("bucket")
    region = args.region or os.getenv("SVDB_REGION") or file_config.get("region")

    commands_without_bucket = ["setup", "configure"]
    if args.command not in commands_without_bucket and not bucket:
        parser.error(
            "Bucket not provided. Use --bucket, set SVDB_BUCKET, or run 'blocks-db configure'."
        )

    client = None
    if args.command not in commands_without_bucket:
        client = VectorDBClient(bucket=bucket, region=region)

    # ── setup ────────────────────────────────────────────────
    if args.command == "setup":
        overrides = {}
        if args.runtime_name:
            overrides["runtime_name"] = args.runtime_name
        if args.function_name:
            overrides["lambda_function_name"] = args.function_name
        if args.table_name:
            overrides["dynamodb_table_name"] = args.table_name
        if args.layer_name:
            overrides["layer_name"] = args.layer_name
        if args.role_name:
            overrides["lambda_role_name"] = args.role_name
        if args.threshold:
            overrides["threshold_size_mb"] = args.threshold
        run_setup(
            s3_bucket=args.bucket,
            config_overrides=overrides,
            create_vector_table=not args.skip_vector_table,
            build_runtime=not args.skip_runtime,
        )

    elif args.command == "update-threshold":
        from .infra import update_lambda_threshold
        update_lambda_threshold(
            threshold_bytes=args.threshold_bytes,
            dataset_name=args.dataset,
            s3_bucket=args.bucket
        )

    elif args.command == "refresh-credentials":
        refresh_lithops_credentials()

    elif args.command == "deploy-lambda":
        from .infra import deploy_lambda_code
        deploy_lambda_code(args.function)

    elif args.command == "configure":
        CONFIG_DIR.mkdir(exist_ok=True)
        with open(CONFIG_FILE, "w") as f:
            json.dump({"bucket": args.bucket, "region": args.region}, f, indent=4)
        print(f"Configuration saved to {CONFIG_FILE}")

    # ── initialize-database ───────────────────────────────────
    elif args.command == "initialize-database":
        print(f"\n=== Uploading dataset '{args.name}' ===")

        client.create_dataset(args.name, args.csv_path)
        print(f"Dataset uploaded.")

        with open(args.config) as f:
            config = json.load(f)

        print(f"\n=== Building index ===")
        times = client.index_dataset(
            dataset_name=args.name,
            config=config,
            num_workers=args.workers
        )
        print(f"Index built successfully.")
        print(f"Timing: {json.dumps(times, indent=2)}")

        if not args.no_update_threshold:
            print(f"\n=== Updating auto-indexer threshold ===")
            from .infra import update_lambda_threshold
            update_lambda_threshold(
                threshold_bytes=None,
                dataset_name=args.name,
                s3_bucket=bucket
            )

    # ── put ───────────────────────────────────────────────────
    elif args.command == "put":
        from .utils.vector_utils import load_vectors_with_ids_from_csv, load_vectors_from_csv

        print(f"\n=== Putting vectors into '{args.name}' ===")

        if args.single:
            with open(args.csv_path) as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or not row[0].strip():
                        continue
                    try:
                        vec_id = int(row[0])
                        vec = [float(x) for x in row[1].strip().split() if x]
                        key = client.tracker.put_vector(args.name, vec_id, vec)
                        print(f"  Uploaded vector {vec_id} -> {key}")
                    except (ValueError, IndexError) as e:
                        print(f"  Skipping invalid line: {row} ({e})")
        else:
            vectors = load_vectors_with_ids_from_csv(args.csv_path)

            if not vectors:
                print("No vectors found in file.")
                return

            if len(vectors) == 1:
                vec_id, vec = vectors[0]
                key = client.tracker.put_vector(args.name, vec_id, vec)
                print(f"Uploaded 1 vector -> {key}")
            else:
                key = client.tracker.put_vectors(args.name, vectors)
                print(f"Uploaded {len(vectors)} vectors -> {key}")

        print(f"\nVectors added to pending. Use 'blocks-db query {args.name}' to search them.")

    # ── query ─────────────────────────────────────────────────
    elif args.command == "query":
        print(f"\n=== Querying '{args.name}' ===")

        hybrid = not args.indexed_only

        if args.vector:
            vector = [float(x) for x in args.vector.split()]
            results, times = client.query(args.name, vector, k=args.k, hybrid=hybrid)
            print(f"Query: {vector[:5]}...")
            print(f"Results: {results[:args.k]}")
        elif args.file:
            if not os.path.exists(args.file):
                raise FileNotFoundError(args.file)
            results, times = client.query_from_file(args.name, args.file, hybrid=hybrid, k=args.k)
            print(f"Results ({len(results)} queries):")
            for i, res in enumerate(results[:5]):
                print(f"  Query {i}: {res[:args.k]}")
            if len(results) > 5:
                print(f"  ... and {len(results) - 5} more queries")

        print(f"\nSearch mode: {'hybrid (indexed + pending)' if hybrid else 'indexed only'}")
        if "error" not in times:
            print(f"Query times: {json.dumps(times, indent=2)}")

    # ── status ────────────────────────────────────────────────
    elif args.command == "status":
        print(f"\n=== Status for '{args.name}' ===\n")

        has_index = len(client.list_indexes(args.name)) > 0
        has_pending = client.has_pending_vectors(args.name)
        
        # Use DynamoDB counter for count (faster)
        indexed_count = client.get_indexed_count(args.name)
        if indexed_count == 0:
            # Fall back to S3 tracking
            indexed_ids = client.get_indexed_ids(args.name)
            indexed_count = len(indexed_ids)

        print(f"Dataset: {args.name}")
        print(f"  Indexed: {'YES' if has_index else '✗'}")
        print(f"  Pending vectors: {'YES' if has_pending else 'NO' if has_index else 'Status failed'}")
        print(f"  Indexed vectors: {indexed_count}")

        if has_pending:
            pending_files = client.tracker.get_pending_files(args.name)
            print(f"  Pending files: {len(pending_files)}")

        if args.verbose:
            indexed_ids = client.get_indexed_ids(args.name)
            print(f"\n  Indexed IDs sample: {sorted(indexed_ids)[:10]}...")

            if has_pending:
                pending = client.get_pending_vectors(args.name)
                print(f"\n  Pending vectors: {len(pending)}")
                for vid, vec in pending[:3]:
                    print(f"    ID {vid}: {vec[:5]}...")
                if len(pending) > 3:
                    print(f"    ... and {len(pending) - 3} more")

    # ── get ───────────────────────────────────────────────────
    elif args.command == "get":
        if args.pending:
            pending = client.get_pending_vectors(args.name)
            if pending:
                limit = args.limit or 10
                print(f"\nPending vectors for '{args.name}' ({len(pending)}):")
                for vid, vec in pending[:limit]:
                    print(f"  ID {vid}: {vec[:5]}...")
                if len(pending) > limit:
                    print(f"  ... and {len(pending) - limit} more")
            else:
                print(f"No pending vectors for '{args.name}'.")
        elif args.ids:
            vectors = client.get_vectors(args.name, args.ids)
            if not vectors:
                print("No vectors found for the given IDs.")
                return

            has_duplicates = False
            for vid in args.ids:
                if vid in vectors and isinstance(vectors[vid], list):
                    has_duplicates = True
                    break

            if has_duplicates:
                print(f"\nFound {len(vectors)} vector(s), {sum(1 for v in vectors.values() if isinstance(v, list))} with duplicates:")
            else:
                print(f"\nFound {len(vectors)} vector(s):")

            for vid in args.ids:
                if vid not in vectors:
                    print(f"  ID {vid}: NOT FOUND")
                    continue

                result = vectors[vid]

                if isinstance(result, list):
                    print(f"  ID {vid}: [DUPLICATES FOUND]")
                    for i, entry in enumerate(result):
                        vec_preview = entry["vector"][:5]
                        source = entry["source"]
                        file_info = f" ({entry['file']})" if source == "pending" else ""
                        print(f"    [{i+1}] {vec_preview}... ({source}{file_info})")
                else:
                    vec_preview = result["vector"][:5]
                    source = result["source"]
                    print(f"  ID {vid}: {vec_preview}... ({source})")
        elif args.limit:
            vectors = client.list_vectors(args.name, limit=args.limit)
            if vectors:
                print(f"\nFirst {len(vectors)} vectors in '{args.name}':")
                for vid, vec in vectors.items():
                    print(f"  ID {vid}: {vec[:5]}...")
            else:
                print(f"No vectors in '{args.name}'.")
        else:
            print("Specify vector IDs, --limit, or --pending")


if __name__ == "__main__":
    main()
