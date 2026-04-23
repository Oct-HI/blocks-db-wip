import argparse
import csv
import json
import os
from pathlib import Path

import boto3
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
    setup_parser.add_argument("--stack-name", default=None, help="CloudFormation stack name")
    setup_parser.add_argument("--runtime-name", default=None, help="Runtime image name in ECR")
    setup_parser.add_argument("--function-name", default=None, help="Lambda function name")
    setup_parser.add_argument("--table-name", default=None, help="DynamoDB table name")
    setup_parser.add_argument("--layer-name", default=None, help="Lambda layer name")
    setup_parser.add_argument("--role-name", default=None, help="Lambda execution role name")
    setup_parser.add_argument("--threshold", type=int, default=None, help="Auto-indexer threshold in bytes (default: 5242880)")
    setup_parser.add_argument("--skip-vector-table", action="store_true", help="Skip DynamoDB table creation")
    setup_parser.add_argument("--skip-runtime", action="store_true", help="Skip Lithops runtime build")

    # ── deploy-runtime ─────────────────────────────────���─────
    runtime_parser = subparsers.add_parser("deploy-runtime", help="Deploy/rebuild Lithops runtime")
    runtime_parser.add_argument("--name", default=None, help="Runtime image name")

    # ── deploy-lambda ───────────────────────────────────
    deploy_lambda_parser = subparsers.add_parser("deploy-lambda", help="Deploy/update Lambda code")
    deploy_lambda_parser.add_argument("--function", default=None, help="Lambda function name")

    # ── quick-deploy-lambda ────────────────────────────────
    quick_deploy_parser = subparsers.add_parser("quick-deploy-lambda", help="Quick deploy: create layer + Lambda (no CloudFormation)")
    quick_deploy_parser.add_argument("--bucket", required=True, help="S3 bucket for storage")
    quick_deploy_parser.add_argument("--threshold", type=int, default=None, help="Threshold in bytes")
    quick_deploy_parser.add_argument("--starting-index", type=int, default=0, help="Starting index number for auto-indexer")

    # ── fix-s3-trigger ──────────────────────────────────
    fix_trigger_parser = subparsers.add_parser("fix-s3-trigger", help="Fix S3 trigger for Lambda")
    fix_trigger_parser.add_argument("--bucket", required=True, help="S3 bucket")

    # ── update-threshold ─────────────────────────────────────
    threshold_parser = subparsers.add_parser("update-threshold", help="Update auto-indexer block size threshold")
    threshold_parser.add_argument("threshold_bytes", nargs="?", type=int, default=None, help="Threshold in bytes (optional, auto-calculate if not provided)")
    threshold_parser.add_argument("--dataset", default=None, help="Dataset name (for auto-calculate)")
    threshold_parser.add_argument("--bucket", default=None, help="S3 bucket (required if --dataset used)")

    subparsers.add_parser("refresh-credentials", help="Refresh AWS credentials")

    configure_parser = subparsers.add_parser("configure", help="Save default bucket and region")
    configure_parser.add_argument("--bucket", required=True, help="Default S3 bucket")
    configure_parser.add_argument("--region", default="us-east-1", help="AWS region")

    # ═══════════════════════════════════════════════════════════
    # BASE OPERATIONS
    # ═══════════════════════════════════════════════════════════

    # ── upload-data ───────────────────────────────────────────
    upload_parser = subparsers.add_parser("upload-data", help="Upload initial dataset and create index")
    upload_parser.add_argument("name", help="Dataset name")
    upload_parser.add_argument("csv_path", help="Path to CSV file with vectors")
    upload_parser.add_argument("--config", required=True, help="Path to index config JSON")
    upload_parser.add_argument("--workers", type=int, default=16, help="Number of indexing workers")
    upload_parser.add_argument("--no-update-threshold", action="store_true", help="Skip auto-update threshold after indexing")

    # ── put ───────────────────────────────────────────────────
    put_parser = subparsers.add_parser("put", help="Add new vectors (stored as individual files)")
    put_parser.add_argument("name", help="Dataset name")
    put_parser.add_argument("csv_path", help="CSV file with vectors (id, vector values)")
    put_parser.add_argument("--single", action="store_true", help="Treat as single vector per file (one vector per file)")

    # ── index ─────────────────────────────────────────────────
    index_parser = subparsers.add_parser("index", help="Build/rebuild index for dataset")
    index_parser.add_argument("name", help="Dataset name")
    index_parser.add_argument("--config", help="Path to index config JSON (uses stored if not provided)")
    index_parser.add_argument("--workers", type=int, default=16, help="Number of indexing workers")
    index_parser.add_argument("--include-pending", action="store_true", help="Include pending vectors in index")

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

    # ═══════════════════════════════════════════════════════════
    # ADDITIONAL OPERATIONS
    # ═══════════════════════════════════════════════════════════

    # ── list ─────────────────────────────────────────────────
    list_parser = subparsers.add_parser("list", help="List datasets or indexes")
    list_parser.add_argument("type", choices=["datasets", "indexes"], help="What to list")
    list_parser.add_argument("name", nargs="?", help="Dataset name (required for indexes)")

    # ── get ───────────────────────────────────────────────────
    get_parser = subparsers.add_parser("get", help="Get vectors by ID")
    get_parser.add_argument("name", help="Dataset name")
    get_parser.add_argument("ids", nargs="+", type=int, help="Vector IDs")

    # ── get-vectors ─────────────────────────────────────────
    get_vectors_parser = subparsers.add_parser("get-vectors", help="Get vectors from dataset (main CSV + pending)")
    get_vectors_parser.add_argument("name", help="Dataset name")
    get_vectors_parser.add_argument("--ids", nargs="+", type=int, help="Specific vector IDs (gets from main CSV)")
    get_vectors_parser.add_argument("--limit", type=int, help="Limit number of vectors (gets from main CSV)")
    get_vectors_parser.add_argument("--pending", action="store_true", help="Get pending vectors instead of main")

    # ── delete ───────────────────────────────────────────────
    delete_parser = subparsers.add_parser("delete", help="Delete dataset or vectors")
    delete_subparsers = delete_parser.add_subparsers(dest="delete_type")

    delete_dataset_parser = delete_subparsers.add_parser("dataset", help="Delete entire dataset")
    delete_dataset_parser.add_argument("name", help="Dataset name")

    delete_vectors_parser = delete_subparsers.add_parser("vectors", help="Delete specific vectors")
    delete_vectors_parser.add_argument("name", help="Dataset name")
    delete_vectors_parser.add_argument("ids", nargs="+", type=int, help="Vector IDs")
    delete_vectors_parser.add_argument("--reindex", action="store_true", default=True, help="Reindex after deletion")
    delete_vectors_parser.add_argument("--no-reindex", action="store_true", help="Skip reindex")

    # ── pending ───────────────────────────────────────────────
    pending_parser = subparsers.add_parser("pending", help="Manage pending vectors")
    pending_subparsers = pending_parser.add_subparsers(dest="pending_action")

    pending_list_parser = pending_subparsers.add_parser("list", help="List pending vectors")
    pending_list_parser.add_argument("name", help="Dataset name")
    pending_list_parser.add_argument("--limit", type=int, default=10, help="Max vectors to show")

    pending_clear_parser = pending_subparsers.add_parser("clear", help="Clear pending vectors")
    pending_clear_parser.add_argument("name", help="Dataset name")

    pending_reindex_parser = pending_subparsers.add_parser("reindex", help="Rebuild index with pending vectors")
    pending_reindex_parser.add_argument("name", help="Dataset name")
    pending_reindex_parser.add_argument("--config", help="Path to index config JSON")
    pending_reindex_parser.add_argument("--workers", type=int, default=16, help="Number of workers")

    # ── config ───────────────────────────────────────────────
    config_parser = subparsers.add_parser("config", help="Manage index configurations")
    config_subparsers = config_parser.add_subparsers(dest="config_action")

    config_save_parser = config_subparsers.add_parser("save", help="Save index config")
    config_save_parser.add_argument("name", help="Dataset name")
    config_save_parser.add_argument("config_path", help="Path to config JSON")

    config_delete_parser = config_subparsers.add_parser("delete", help="Delete index configs")
    config_delete_parser.add_argument("name", help="Dataset name")

    args = parser.parse_args()

    VISIBLE_COMMANDS = {
        "setup", "configure", "refresh-credentials", "upload-data", "put", "query", "status", "update-threshold", "config"
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

    # ═══════════════════════════════════════════════════════════
    # COMMAND HANDLERS
    # ═══════════════════════════════════════════════════════════

    # ── setup ────────────────────────────────────────────────
    if args.command == "setup":
        overrides = {}
        if args.stack_name:
            overrides["stack_name"] = args.stack_name
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

    # TODO: Legacy - may not be used
    elif args.command == "deploy-lambda":
        from .infra import deploy_lambda_code
        deploy_lambda_code(args.function)

    # TODO: Legacy - may not be used
    elif args.command == "quick-deploy-lambda":
        overrides = {"threshold_size_bytes": args.threshold} if args.threshold else None
        #quick_deploy_lambda(args.bucket, overrides=overrides, starting_index=args.starting_index)

    # TODO: Legacy - may not be used
    elif args.command == "fix-s3-trigger":
        import boto3
        from .infra import configure_s3_notification
        lambda_client = boto3.client("lambda")
        config = get_infra_config()
        function_name = config.get("lambda_function_name")
        response = lambda_client.get_function_configuration(FunctionName=function_name)
        lambda_arn = response["FunctionArn"]
        configure_s3_notification(args.bucket, lambda_arn, function_name)

    elif args.command == "configure":
        CONFIG_DIR.mkdir(exist_ok=True)
        with open(CONFIG_FILE, "w") as f:
            json.dump({"bucket": args.bucket, "region": args.region}, f, indent=4)
        print(f"Configuration saved to {CONFIG_FILE}")

    # ── upload-data ───────────────────────────────────────────
    elif args.command == "upload-data":
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

    # TODO: Legacy - hidden command
    elif args.command == "index":
        print(f"\n=== Building index for '{args.name}' ===")
        
        config = None
        if args.config:
            with open(args.config) as f:
                config = json.load(f)
        
        if args.include_pending and client.has_pending_vectors(args.name):
            print("Including pending vectors in index...")
            pending = client.get_pending_vectors(args.name)
            pending_ids = [v[0] for v in pending]
            
            from .utils.vector_utils import load_vectors_from_csv
            main_csv = f"vectors_{args.name}.csv"
            
            if config is None:
                indexes = client.list_indexes(args.name)
                if indexes:
                    impl, num = indexes[0]
                    config = client._load_index_config_for_search(args.name)
            
            print(f"Will merge {len(pending)} pending vectors with main dataset...")
        
        times = client.index_dataset(
            dataset_name=args.name,
            config=config,
            num_workers=args.workers
        )
        
        if args.include_pending and pending:
            client.tracker.clear_pending(args.name)
            indexed = client.get_indexed_ids(args.name)
            print(f"Indexed {len(indexed)} total vectors (including pending).")
        
        print(f"Index built successfully.")
        print(f"Timing: {json.dumps(times, indent=2)}")

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
        indexed_ids = client.get_indexed_ids(args.name)
        
        print(f"Dataset: {args.name}")
        print(f"  Indexed: {'YES' if has_index else '✗'}")
        print(f"  Pending vectors: {'YES' if has_pending else 'NO' if has_index else 'Status failed'}")
        print(f"  Indexed vectors: {len(indexed_ids)}")
        
        if has_pending:
            pending_files = client.tracker.get_pending_files(args.name)
            print(f"  Pending files: {len(pending_files)}")
        
        if args.verbose:
            print(f"\n  Indexed IDs sample: {sorted(indexed_ids)[:10]}...")
            
            if has_pending:
                pending = client.get_pending_vectors(args.name)
                print(f"\n  Pending vectors: {len(pending)}")
                for vid, vec in pending[:3]:
                    print(f"    ID {vid}: {vec[:5]}...")
                if len(pending) > 3:
                    print(f"    ... and {len(pending) - 3} more")

    # TODO: Hidden command
    # ── list ──────────────────────────────────────────────────
    elif args.command == "list":
        if args.type == "datasets":
            datasets = client.list_datasets()
            if datasets:
                print(f"\nDatasets ({len(datasets)}):")
                for ds in datasets:
                    print(f"  - {ds}")
            else:
                print("No datasets found.")
        elif args.type == "indexes":
            if not args.name:
                print("Error: dataset name required for 'list indexes'")
                return
            indexes = client.list_indexes(args.name)
            if indexes:
                print(f"\nIndexes for '{args.name}':")
                for impl, num in indexes:
                    print(f"  - implementation={impl}, num_index={num}")
            else:
                print(f"No indexes found for '{args.name}'.")

    # TODO: Hidden command
    # ── get ───────────────────────────────────────────────────
    elif args.command == "get":
        from .utils.query_ops import get_vectors_by_id
        vectors = get_vectors_by_id(bucket, args.name, args.ids)
        if vectors:
            print(f"\nFound {len(vectors)} vectors:")
            for vid, vec in vectors.items():
                print(f"  ID {vid}: {vec[:5]}...")
        else:
            print("No vectors found for the given IDs.")

    # TODO: Hidden command
    # ── get-vectors ─────────────────────────────────────────
    elif args.command == "get-vectors":
        if args.pending:
            pending = client.get_pending_vectors(args.name)
            if pending:
                print(f"\nPending vectors for '{args.name}' ({len(pending)}):")
                for vid, vec in pending[:args.limit or 10]:
                    print(f"  ID {vid}: {vec[:5]}...")
            else:
                print(f"No pending vectors for '{args.name}'.")
        elif args.ids:
            vectors = client.get_vectors(args.name, args.ids)
            if vectors:
                print(f"\nFound {len(vectors)} vectors:")
                for vid, vec in vectors.items():
                    print(f"  ID {vid}: {vec[:5]}...")
            else:
                print("No vectors found for the given IDs.")
        elif args.limit:
            vectors = client.list_vectors(args.name, limit=args.limit)
            if vectors:
                print(f"\nFirst {len(vectors)} vectors in '{args.name}':")
                for vid, vec in vectors.items():
                    print(f"  ID {vid}: {vec[:5]}...")
            else:
                print(f"No vectors in '{args.name}'.")
        else:
            print("Specify --ids, --limit, or --pending")

    # TODO: Hidden command
    # ── delete ────────────────────────────────────────────────
    elif args.command == "delete":
        if args.delete_type == "dataset":
            confirm = input(f"Delete dataset '{args.name}' and all its data? (y/n): ")
            if confirm.lower() == "y":
                client.delete_dataset(args.name)
                print(f"Dataset '{args.name}' deleted.")
        elif args.delete_type == "vectors":
            reindex = not args.no_reindex
            from .utils.dataset_ops import delete_vectors_from_dataset
            delete_vectors_from_dataset(bucket, args.name, args.ids, reindex=reindex)
            print(f"Deleted {len(args.ids)} vectors. Reindex={'yes' if reindex else 'no'}")

    # TODO: Hidden command
    # ── pending ───────────────────────────────────────────────
    elif args.command == "pending":
        if args.pending_action == "list":
            pending = client.get_pending_vectors(args.name)
            if pending:
                print(f"\nPending vectors for '{args.name}' ({len(pending)}):")
                for vid, vec in pending[:args.limit]:
                    print(f"  ID {vid}: {vec[:5]}...")
                if len(pending) > args.limit:
                    print(f"  ... and {len(pending) - args.limit} more")
            else:
                print(f"No pending vectors for '{args.name}'.")
        elif args.pending_action == "clear":
            confirm = input(f"Clear all pending vectors for '{args.name}'? (y/n): ")
            if confirm.lower() == "y":
                client.tracker.clear_pending(args.name)
                print(f"Pending vectors cleared.")
        elif args.pending_action == "reindex":
            config = None
            if args.config:
                with open(args.config) as f:
                    config = json.load(f)
            times = client.reindex_pending(args.name, config=config, num_workers=args.workers)
            print(f"Reindexing completed. Timing: {json.dumps(times, indent=2)}")

    # ── config ────────────────────────────────────────────────
    elif args.command == "config":
        if args.config_action == "save":
            with open(args.config_path) as f:
                config = json.load(f)
            client.save_index_config(args.name, config)
            print(f"Config saved for '{args.name}'.")
        elif args.config_action == "delete":
            client.delete_index_configs(args.name)
            print(f"Index configs deleted for '{args.name}'.")


if __name__ == "__main__":
    main()
