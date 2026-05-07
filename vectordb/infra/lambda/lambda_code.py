"""
Lambda auto-indexer for Blocks-DB.

This Lambda function is triggered by S3 PUT events on the pending vectors CSV.
It accumulates vectors until a threshold is reached, then builds a FAISS index.

Threshold can be set via:
1. S3 object metadata (x-amz-meta-threshold-mb) - per-upload
2. Environment variable THRESHOLD_SIZE_MB - default
3. Event data (threshold_mb in the event json) - per-invocation
"""

import boto3
import csv
import io
import json
import os
import time
from typing import List, Dict, Any

import faiss
import numpy as np
from botocore.exceptions import ClientError

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "__DYNAMODB_TABLE__")
DEFAULT_THRESHOLD_BYTES = int(os.environ.get("THRESHOLD_SIZE_BYTES", "__THRESHOLD_SIZE__"))
INDEX_IMPLEMENTATION = os.environ.get("INDEX_IMPLEMENTATION", "blocks")
BYTES_PER_VECTOR = 8 + 96 * 8

_configured_starting_index = None


def get_threshold_bytes(event: Dict[str, Any], s3_metadata: Dict[str, str] = None) -> int:
    """Get threshold in bytes from multiple sources (in priority order):
    1. Event data (threshold_bytes)
    2. S3 object metadata (x-amz-meta-threshold-bytes)
    3. Environment variable THRESHOLD_SIZE_BYTES (DEFAULT_THRESHOLD_BYTES)
    """
    if event and "threshold_bytes" in event:
        return int(event["threshold_bytes"])
    
    if s3_metadata:
        threshold_meta = s3_metadata.get("threshold-bytes") or s3_metadata.get("threshold_bytes")
        if threshold_meta:
            return int(threshold_meta)
    
    return DEFAULT_THRESHOLD_BYTES


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle S3 PUT events for vector files."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    
    threshold_bytes = get_threshold_bytes(event)
    print(f"Using threshold: {threshold_bytes:,} bytes")
    
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        if not key.endswith(".csv"):
            continue
        parts = key.split("/")
        if len(parts) > 1:
            dataset = parts[1]
            ensure_global_metadata(table, bucket, dataset)
            break
    
    ensure_global_metadata(table)
    
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        if not key.endswith(".csv"):
            continue
        
        s3_metadata = {}
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            s3_metadata = head.get("Metadata", {})
        except Exception:
            pass
        
        record_threshold = get_threshold_bytes(event, s3_metadata)
        if record_threshold != threshold_bytes:
            threshold_bytes = record_threshold
            print(f"Updated threshold from metadata: {threshold_bytes:,} bytes")
        
        try:
            result = accumulate_file(bucket, key, table)
            print(f"Accumulated {key}: {result}")
        except Exception as e:
            print(f"Error accumulating {key}: {e}")
            raise
    
    response = table.get_item(
        Key={"centroid_id": "GLOBAL_CONFIG", "sk": "META"}
    )
    item = response.get("Item", {})
    total_size = int(item.get("current_accumulated_size", 0))
    current_centroid_id = int(item.get("current_centroid_id", 0))
    
    print(f"State before indexing: centroid_id={current_centroid_id}, accumulated={total_size} bytes, threshold={threshold_bytes}")
    
    if total_size >= threshold_bytes:
        print(f"Threshold reached, triggering indexing for centroid {current_centroid_id}")
        
        try:
            response = table.update_item(
                Key={"centroid_id": "GLOBAL_CONFIG", "sk": "META"},
                UpdateExpression="SET current_centroid_id = current_centroid_id + :inc, current_accumulated_size = :zero",
                ConditionExpression="current_centroid_id = :old_id",
                ExpressionAttributeValues={
                    ":inc": 1,
                    ":zero": 0,
                    ":old_id": current_centroid_id
                },
                ReturnValues="ALL_NEW"
            )
            
            new_centroid_id = int(response["Attributes"]["current_centroid_id"])
            print(f"Successfully claimed centroid {current_centroid_id} (now {new_centroid_id})")
            
            files = get_files_for_centroid(current_centroid_id, table)
            if files:
                dataset = files[0].get("dataset", "default")
                create_index_for_centroid(current_centroid_id, bucket, dataset, table)
                move_indexed_files_to_processed(bucket, table, current_centroid_id)
                print(f"Index created for centroid {current_centroid_id}")
            else:
                print(f"No files found for centroid {current_centroid_id}")
            
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                print("Another worker already started indexing - skipping")
            else:
                raise
    
    return {"statusCode": 200, "body": "OK"}


def accumulate_file(bucket: str, key: str, table):
    """Accumulate file size in DynamoDB."""
    head = s3.head_object(Bucket=bucket, Key=key)
    file_size = head["ContentLength"]
    
    response = table.update_item(
        Key={"centroid_id": "GLOBAL_CONFIG", "sk": "META"},
        UpdateExpression="SET current_accumulated_size = if_not_exists(current_accumulated_size, :zero) + :s",
        ExpressionAttributeValues={":s": file_size, ":zero": 0},
        ReturnValues="ALL_NEW"
    )
    
    attrs = response["Attributes"]
    current_centroid_id = int(attrs.get("current_centroid_id", 0))
    total_size = int(attrs.get("current_accumulated_size", 0))
    
    parts = key.split("/")
    dataset_name = parts[1] if len(parts) > 1 else "default"
    
    table.put_item(Item={
        "centroid_id": str(current_centroid_id),
        "sk": f"FILE#{key}",
        "prefix": "/".join(parts[:-1]),
        "file_key": parts[-1],
        "dataset": dataset_name,
        "size": file_size,
        "timestamp": int(time.time() * 1000)
    })
    
    return {"centroid": current_centroid_id, "total_size": total_size, "dataset": dataset_name}


def get_starting_index_from_config(bucket, dataset):
    """Read num_index from S3 config to determine starting index for auto-indexing."""
    global _configured_starting_index
    if _configured_starting_index is not None:
        return _configured_starting_index
    
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=f"indexes/{dataset}/blocks/")
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith("config.json"):
                config_response = s3.get_object(Bucket=bucket, Key=key)
                import orjson
                config = orjson.loads(config_response["Body"].read())
                num_index = config.get("num_index", 16)
                print(f"Read config from {key}: num_index={num_index}")
                _configured_starting_index = num_index
                return num_index
    except Exception as e:
        print(f"No config found, starting from 0: {e}")
    
    _configured_starting_index = 0
    return 0


def ensure_global_metadata(table, bucket=None, dataset=None):
    """Ensure the GLOBAL_CONFIG metadata item exists with initial values."""
    if bucket and dataset and _configured_starting_index is None:
        get_starting_index_from_config(bucket, dataset)
    
    try:
        response = table.update_item(
            Key={"centroid_id": "GLOBAL_CONFIG", "sk": "META"},
            UpdateExpression="SET current_accumulated_size = if_not_exists(current_accumulated_size, :zero), current_centroid_id = if_not_exists(current_centroid_id, :zero)",
            ExpressionAttributeValues={":zero": 0},
            ConditionExpression="attribute_not_exists(current_accumulated_size)",
            ReturnValues="ALL_NEW"
        )
        print(f"Initialized GLOBAL_CONFIG: centroid_id={response['Attributes'].get('current_centroid_id')}, accumulated={response['Attributes'].get('current_accumulated_size')}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            print(f"Warning initializing global metadata (may already exist): {e}")
    except Exception as e:
        print(f"Warning initializing global metadata: {e}")


def move_indexed_files_to_processed(bucket: str, table, centroid_id: int):
    """Delete processed files from pending/ (the processed batch is written separately with corrected IDs)."""
    files = get_files_for_centroid(centroid_id, table)
    if not files:
        return
    
    for f in files:
        try:
            source_key = f"{f['prefix']}/{f['file_key']}"
            s3.delete_object(Bucket=bucket, Key=source_key)
            print(f"Deleted processed pending file: {source_key}")
        except Exception as e:
            print(f"Error deleting file {f.get('file_key')}: {e}")


def save_processed_batch(bucket: str, dataset: str, centroid_id: int, new_ids: List[int], vectors: List[List[float]]) -> None:
    """Write a batch CSV with corrected IDs to processed/."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["id", "vector"])
    for vid, vec in zip(new_ids, vectors):
        writer.writerow([vid, " ".join(str(x) for x in vec)])

    batch_key = f"processed/{dataset}/batch_{centroid_id}.csv"
    s3.put_object(Bucket=bucket, Key=batch_key, Body=buffer.getvalue().encode("utf-8"))
    print(f"Saved processed batch: {batch_key} with {len(vectors)} vectors, IDs {new_ids[0]}-{new_ids[-1]}")


def create_index_for_centroid(centroid_id: int, bucket: str, dataset: str, table) -> None:
    """Build and store FAISS index for a centroid."""
    files = get_files_for_centroid(centroid_id, table)
    
    if not files:
        print(f"No files found for centroid {centroid_id}")
        return
    
    original_ids = []
    vectors = []
    features = None
    
    start = time.time()
    for f in files:
        key = f"{f['prefix']}/{f['file_key']}"
        try:
            body = s3.get_object(Bucket=bucket, Key=key)["Body"]
            for row in csv.reader(io.TextIOWrapper(body, encoding="utf-8")):
                if len(row) < 2:
                    continue
                first_col = row[0].strip().lower()
                if first_col == "" or first_col == "id":
                    continue
                try:
                    vec_id = int(float(first_col))
                except:
                    continue
                try:
                    vec = [float(x) for x in row[1].strip().split() if x]
                except:
                    continue
                if not vec:
                    continue
                if features is None:
                    features = len(vec)
                original_ids.append(vec_id)
                vectors.append(vec)
        except Exception as e:
            print(f"Error reading file {key}: {e}")
    
    load_time = time.time() - start
    print(f"Loaded {len(vectors)} vectors in {load_time:.2f}s")
    
    if not vectors or features is None:
        return
    
    # Use atomic DynamoDB counter to get and reserve IDs
    next_available_id = get_next_available_id_atomic(bucket, dataset, len(original_ids))
    new_ids = list(range(next_available_id, next_available_id + len(original_ids)))
    print(f"Reassigning IDs: {len(original_ids)} vectors from IDs {min(original_ids)}-{max(original_ids)} to {next_available_id}-{next_available_id + len(new_ids) - 1}")
    
    start = time.time()
    k = max(1, min(4096, len(vectors) // 4))
    n_probe = min(1024, k)
    
    index = faiss.index_factory(features, f"IVF{k},Flat")
    index.train(np.array(vectors, dtype="float32"))
    index.nprobe = n_probe
    index.add_with_ids(np.array(vectors, dtype="float32"), np.array(new_ids))
    train_time = time.time() - start
    print(f"Built index in {train_time:.2f}s")
    
    start = time.time()
    faiss.write_index(index, f"/tmp/c{centroid_id}.ann")
    
    s3.upload_file(
        f"/tmp/c{centroid_id}.ann",
        bucket,
        f"indexes/{dataset}/{INDEX_IMPLEMENTATION}/centroid_{centroid_id}.ann"
    )
    store_time = time.time() - start
    print(f"Stored index at indexes/{dataset}/{INDEX_IMPLEMENTATION}/centroid_{centroid_id}.ann in {store_time:.2f}s")
    
    update_indexed_tracking(bucket, dataset, new_ids)

    save_processed_batch(bucket, dataset, centroid_id, new_ids, vectors)
    
    update_config_num_index(bucket, dataset, centroid_id + 1)


def update_config_num_index(bucket: str, dataset: str, num_index: int):
    """Update num_index in S3 config after creating a new index block."""
    config_key = f"indexes/{dataset}/{INDEX_IMPLEMENTATION}/config.json"
    try:
        existing = s3.get_object(Bucket=bucket, Key=config_key)
        config = json.loads(existing["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"No existing config to update: {e}")
        return
    
    old_num_index = config.get("num_index", 0)
    if num_index > old_num_index:
        config["num_index"] = num_index
        s3.put_object(
            Bucket=bucket,
            Key=config_key,
            Body=json.dumps(config),
            ContentType="application/json"
        )
        print(f"Updated config num_index: {old_num_index} -> {num_index}")
    else:
        print(f"Config num_index unchanged: {old_num_index}")


def get_next_available_id_atomic(bucket: str, dataset: str, count: int) -> int:
    """Atomically get and reserve the next `count` IDs from DynamoDB. Returns the starting ID."""
    global dynamodb
    if isinstance(dynamodb, boto3.resources.factory.ServiceResource):
        table = dynamodb.Table(DYNAMODB_TABLE)
    else:
        # Re-initialize if needed
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(DYNAMODB_TABLE)
    
    try:
        response = table.update_item(
            Key={"centroid_id": "ID_TRACKER", "sk": dataset},
            UpdateExpression="SET next_id = if_not_exists(next_id, :zero) + :inc",
            ExpressionAttributeValues={":inc": count, ":zero": 0},
            ReturnValues="ALL_NEW"
        )
        # Return the starting ID (value after increment minus count)
        # Convert Decimal to int since DynamoDB returns Decimal type
        return int(response["Attributes"]["next_id"] - count)
    except Exception as e:
        print(f"Error getting next ID atomically: {e}")
        raise


def update_indexed_tracking(bucket: str, dataset: str, ids: List[int]):
    """Track indexed vector IDs in S3 for status commands (optional with atomic DynamoDB counter)."""
    key = f"indexed_ids_{dataset}.json"
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)
        import orjson
        indexed_set = set(orjson.loads(existing["Body"].read()))
    except:
        indexed_set = set()

    # IDs are already reassigned by create_index_for_centroid()
    indexed_set.update(ids)

    import orjson
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=orjson.dumps(list(indexed_set)),
        ContentType="application/json"
    )

    return ids


def get_files_for_centroid(centroid_id: int, table) -> List[Dict]:
    """Query DynamoDB for all files in a centroid."""
    files = []
    done = False
    start_key = None
    
    while not done:
        kwargs = {"KeyConditionExpression": "centroid_id = :cid"}
        kwargs["ExpressionAttributeValues"] = {":cid": str(centroid_id)}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key
        
        response = table.query(**kwargs)
        files.extend(response.get("Items", []))
        start_key = response.get("LastEvaluatedKey")
        done = start_key is None
    
    return files
