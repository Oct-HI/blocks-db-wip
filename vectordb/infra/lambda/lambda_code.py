"""
Lambda auto-indexer for Blocks-DB.

Triggered by:
- S3 Event Notifications (standard buckets)
- SQS messages (S3 Express One Zone or opt-in buckets)

Accumulates vectors until a threshold is reached, then builds a FAISS index.

Threshold can be set via:
1. S3 object metadata (x-amz-meta-threshold-bytes) - per-upload
2. Environment variable THRESHOLD_SIZE_BYTES - default
3. Event data (threshold_bytes in the event json) - per-invocation
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

DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "BlocksDB-default")
DEFAULT_THRESHOLD_BYTES = int(os.environ.get("THRESHOLD_SIZE_BYTES", "33554432"))
INDEX_IMPLEMENTATION = os.environ.get("INDEX_IMPLEMENTATION", "blocks")
BYTES_PER_VECTOR = 8 + 96 * 8

_configured_starting_index = None


def get_threshold_bytes(event: Dict[str, Any], s3_metadata: Dict[str, str] = None) -> int:
    if event and "threshold_bytes" in event:
        return int(event["threshold_bytes"])

    if s3_metadata:
        threshold_meta = s3_metadata.get("threshold-bytes") or s3_metadata.get("threshold_bytes")
        if threshold_meta:
            return int(threshold_meta)

    return DEFAULT_THRESHOLD_BYTES


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    table = dynamodb.Table(DYNAMODB_TABLE)
    threshold_bytes = get_threshold_bytes(event)
    print(f"Using threshold: {threshold_bytes:,} bytes")

    if "Records" not in event or not event["Records"]:
        return {"statusCode": 200, "body": "OK"}

    first = event["Records"][0]
    if "eventSource" in first and first["eventSource"] == "aws:sqs":
        return _handle_sqs_event(event, table, threshold_bytes)
    else:
        return _handle_s3_event(event, table, threshold_bytes)


def _handle_s3_event(event: Dict[str, Any], table, threshold_bytes: int) -> Dict[str, Any]:
    datasets_seen = set()

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        if not key.endswith(".csv"):
            continue

        parts = key.split("/")
        if len(parts) <= 1:
            continue
        dataset = parts[1]
        datasets_seen.add(dataset)

        s3_metadata = {}
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            s3_metadata = head.get("Metadata", {})
        except Exception:
            pass

        ensure_global_metadata(table, bucket, dataset)

        record_threshold = get_threshold_bytes(event, s3_metadata)
        if record_threshold != threshold_bytes:
            threshold_bytes = record_threshold

        try:
            result = accumulate_file(bucket, key, table, s3_metadata=s3_metadata)
            print(f"Accumulated {key}: {result}")
        except Exception as e:
            print(f"Error accumulating {key}: {e}")
            raise

    first_bucket = event["Records"][0]["s3"]["bucket"]["name"] if datasets_seen else None
    _check_and_index_datasets(datasets_seen, first_bucket, table, threshold_bytes)
    return {"statusCode": 200, "body": "OK"}


def _handle_sqs_event(event: Dict[str, Any], table, threshold_bytes: int) -> Dict[str, Any]:
    datasets_seen = set()
    first_bucket = None

    for record in event.get("Records", []):
        body = json.loads(record["body"])
        bucket = body["bucket"]
        key = body["key"]
        if first_bucket is None:
            first_bucket = bucket

        if not key.endswith(".csv"):
            continue
        parts = key.split("/")
        if len(parts) <= 1:
            continue
        dataset = parts[1]
        datasets_seen.add(dataset)

        file_size = body.get("file_size")
        tags = body.get("tags")
        s3_metadata = {"tags": json.dumps(tags)} if tags else {}

        ensure_global_metadata(table, bucket, dataset)

        try:
            result = accumulate_file(bucket, key, table, file_size=file_size, s3_metadata=s3_metadata)
            print(f"Accumulated {key}: {result}")
        except Exception as e:
            print(f"Error accumulating {key}: {e}")
            raise

    _check_and_index_datasets(datasets_seen, first_bucket, table, threshold_bytes)
    return {"statusCode": 200, "body": "OK"}


def _check_and_index_datasets(datasets_seen: set, bucket: str, table, threshold_bytes: int):
    for dataset in datasets_seen:
        pk = f"{dataset}_CONFIG"
        response = table.get_item(Key={"centroid_id": pk, "sk": "META"})
        item = response.get("Item", {})
        total_size = int(item.get("current_accumulated_size", 0))
        current_centroid_id = int(item.get("current_centroid_id", 0))

        print(f"[{dataset}] State: centroid_id={current_centroid_id}, accumulated={total_size} bytes, threshold={threshold_bytes}")

        if total_size >= threshold_bytes:
            print(f"[{dataset}] Threshold reached, triggering indexing for centroid {current_centroid_id}")

            try:
                response = table.update_item(
                    Key={"centroid_id": pk, "sk": "META"},
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
                print(f"[{dataset}] Claimed centroid {current_centroid_id} (now {new_centroid_id})")

                files = get_files_for_centroid(dataset, current_centroid_id, table)
                if files:
                    create_index_for_centroid(current_centroid_id, bucket, dataset, table)
                    move_indexed_files_to_processed(bucket, table, dataset, current_centroid_id)
                    print(f"[{dataset}] Index created for centroid {current_centroid_id}")
                else:
                    print(f"[{dataset}] No files found for centroid {current_centroid_id}")

            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    print(f"[{dataset}] Another worker already started indexing - skipping")
                else:
                    raise


def accumulate_file(bucket: str, key: str, table, file_size: int = None, s3_metadata: dict = None):
    """Accumulate file size in DynamoDB, carrying over user tags.

    Args:
        file_size: Pre-computed file size (from SQS message). If None, calls s3.head_object().
        s3_metadata: Pre-fetched S3 metadata (from SQS message). If None, calls s3.head_object().
    """
    parts = key.split("/")
    dataset_name = parts[1] if len(parts) > 1 else "default"

    if file_size is None or s3_metadata is None:
        head = s3.head_object(Bucket=bucket, Key=key)
        if file_size is None:
            file_size = head["ContentLength"]
        if s3_metadata is None:
            s3_metadata = head.get("Metadata", {})

    tags_str = s3_metadata.get("tags")
    tags = json.loads(tags_str) if tags_str else None

    pk = f"{dataset_name}_CONFIG"
    response = table.update_item(
        Key={"centroid_id": pk, "sk": "META"},
        UpdateExpression="SET current_accumulated_size = if_not_exists(current_accumulated_size, :zero) + :s",
        ExpressionAttributeValues={":s": file_size, ":zero": 0},
        ReturnValues="ALL_NEW"
    )

    attrs = response["Attributes"]
    current_centroid_id = int(attrs.get("current_centroid_id", 0))
    total_size = int(attrs.get("current_accumulated_size", 0))

    item = {
        "centroid_id": f"{dataset_name}#{current_centroid_id}",
        "sk": f"FILE#{key}",
        "prefix": "/".join(parts[:-1]),
        "file_key": parts[-1],
        "dataset": dataset_name,
        "size": file_size,
        "timestamp": int(time.time() * 1000)
    }
    if tags:
        item["tags"] = json.dumps(tags)
    table.put_item(Item=item)

    return {"centroid": current_centroid_id, "total_size": total_size, "dataset": dataset_name}


def get_starting_index_from_config(bucket, dataset):
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
    if not dataset:
        return
    if bucket and _configured_starting_index is None:
        get_starting_index_from_config(bucket, dataset)

    pk = f"{dataset}_CONFIG"
    try:
        response = table.update_item(
            Key={"centroid_id": pk, "sk": "META"},
            UpdateExpression="SET current_accumulated_size = if_not_exists(current_accumulated_size, :zero), current_centroid_id = if_not_exists(current_centroid_id, :zero)",
            ExpressionAttributeValues={":zero": 0},
            ConditionExpression="attribute_not_exists(current_accumulated_size)",
            ReturnValues="ALL_NEW"
        )
        print(f"Initialized {pk}: centroid_id={response['Attributes'].get('current_centroid_id')}, accumulated={response['Attributes'].get('current_accumulated_size')}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            print(f"Warning initializing {pk} (may already exist): {e}")
    except Exception as e:
        print(f"Warning initializing {pk}: {e}")


def move_indexed_files_to_processed(bucket: str, table, dataset: str, centroid_id: int):
    files = get_files_for_centroid(dataset, centroid_id, table)
    if not files:
        return

    for f in files:
        try:
            source_key = f"{f['prefix']}/{f['file_key']}"
            s3.delete_object(Bucket=bucket, Key=source_key)
            print(f"Deleted processed pending file: {source_key}")
        except Exception as e:
            print(f"Error deleting file {f.get('file_key')}: {e}")
        try:
            table.delete_item(Key={"centroid_id": "PENDING", "sk": f"FILE#{source_key}"})
            print(f"Cleaned up PENDING tracking for {source_key}")
        except Exception as e:
            print(f"Error cleaning up PENDING tracking for {source_key}: {e}")
        try:
            centroid_key = f"{dataset}#{centroid_id}"
            table.delete_item(Key={"centroid_id": centroid_key, "sk": f"FILE#{source_key}"})
            print(f"Cleaned up centroid tracking for {source_key}")
        except Exception as e:
            print(f"Error cleaning up centroid tracking for {source_key}: {e}")


def save_processed_batch(bucket: str, dataset: str, centroid_id: int, new_ids: List[int], vectors: List[List[float]]) -> None:
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["id", "vector"])
    for vid, vec in zip(new_ids, vectors):
        writer.writerow([vid, " ".join(str(x) for x in vec)])

    batch_key = f"processed/{dataset}/batch_{centroid_id}.csv"
    s3.put_object(Bucket=bucket, Key=batch_key, Body=buffer.getvalue().encode("utf-8"))
    print(f"Saved processed batch: {batch_key} with {len(vectors)} vectors, IDs {new_ids[0]}-{new_ids[-1]}")


def create_index_for_centroid(centroid_id: int, bucket: str, dataset: str, table) -> None:
    files = get_files_for_centroid(dataset, centroid_id, table)

    if not files:
        print(f"No files found for centroid {centroid_id}")
        return

    original_ids = []
    vectors = []
    per_vector_tags = []
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

                tags = None
                if len(row) > 2 and row[2].strip():
                    try:
                        tags = json.loads(row[2]) if isinstance(row[2], str) else row[2]
                        if not isinstance(tags, dict):
                            tags = None
                    except (json.JSONDecodeError, ValueError):
                        tags = None
                per_vector_tags.append(tags)
        except Exception as e:
            print(f"Error reading file {key}: {e}")

    load_time = time.time() - start
    print(f"Loaded {len(vectors)} vectors in {load_time:.2f}s")

    if not vectors or features is None:
        return

    next_available_id = get_next_available_id_atomic(bucket, dataset, len(original_ids))
    new_ids = list(range(next_available_id, next_available_id + len(original_ids)))
    print(f"Reassigning IDs: {len(original_ids)} vectors from IDs {min(original_ids)}-{max(original_ids)} to {next_available_id}-{next_available_id + len(new_ids) - 1}")

    tags_dict = {}
    for i, nid in enumerate(new_ids):
        t = per_vector_tags[i] if i < len(per_vector_tags) else None
        if t:
            tags_dict[str(nid)] = t

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

    if tags_dict:
        tags_body = json.dumps(tags_dict).encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=f"indexes/{dataset}/{INDEX_IMPLEMENTATION}/centroid_{centroid_id}_tags.json",
            Body=tags_body,
            ContentType="application/json"
        )
        print(f"Stored tags for centroid {centroid_id}: {len(tags_dict)} vectors tagged")

        reverse = {}
        for vid_str, vt in tags_dict.items():
            for k, v in vt.items():
                reverse.setdefault(f"{k}:{v}", []).append(int(vid_str))
        s3.put_object(
            Bucket=bucket,
            Key=f"indexes/{dataset}/{INDEX_IMPLEMENTATION}/centroid_{centroid_id}_reverse_tags.json",
            Body=json.dumps(reverse).encode("utf-8"),
            ContentType="application/json"
        )

    store_time = time.time() - start
    print(f"Stored index at indexes/{dataset}/{INDEX_IMPLEMENTATION}/centroid_{centroid_id}.ann in {store_time:.2f}s")

    update_indexed_tracking(bucket, dataset, new_ids)

    save_processed_batch(bucket, dataset, centroid_id, new_ids, vectors)

    update_config_num_index(bucket, dataset, centroid_id + 1)

    aggregated = {}
    for vid_str, vt in tags_dict.items():
        for k, v in vt.items():
            aggregated.setdefault(k, set()).add(v)
    if aggregated:
        tags_map = {k: sorted(v) for k, v in aggregated.items()}
        try:
            table.put_item(Item={
                "centroid_id": f"DATASET#{dataset}",
                "sk": f"CENTROID#{centroid_id}#META",
                "tags": tags_map
            })
            print(f"Saved centroid DDB tags for {dataset}/{centroid_id}: {tags_map}")
        except Exception as e:
            print(f"Error saving centroid DDB tags: {e}")


def save_centroid_tags(centroid_id: int, dataset: str, files: List[Dict], table):
    aggregated = {}
    for f in files:
        raw_tags = f.get("tags")
        if not raw_tags:
            continue
        if isinstance(raw_tags, str):
            try:
                tags = json.loads(raw_tags)
            except (json.JSONDecodeError, TypeError):
                continue
        else:
            tags = raw_tags
        if not isinstance(tags, dict):
            continue
        for k, v in tags.items():
            if k not in aggregated:
                aggregated[k] = set()
            aggregated[k].add(v)

    if not aggregated:
        return

    tags_map = {k: list(v) for k, v in aggregated.items()}
    try:
        table.put_item(Item={
            "centroid_id": f"DATASET#{dataset}",
            "sk": f"CENTROID#{centroid_id}#META",
            "tags": tags_map
        })
        print(f"Saved tags for centroids/{dataset}/{centroid_id}: {tags_map}")
    except Exception as e:
        print(f"Error saving centroid tags: {e}")


def update_config_num_index(bucket: str, dataset: str, num_index: int):
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
    global dynamodb
    if isinstance(dynamodb, boto3.resources.factory.ServiceResource):
        table = dynamodb.Table(DYNAMODB_TABLE)
    else:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(DYNAMODB_TABLE)

    try:
        response = table.update_item(
            Key={"centroid_id": f"{dataset}_ID_TRACKER", "sk": "META"},
            UpdateExpression="SET next_id = if_not_exists(next_id, :zero) + :inc",
            ExpressionAttributeValues={":inc": count, ":zero": 0},
            ReturnValues="ALL_NEW"
        )
        return int(response["Attributes"]["next_id"] - count)
    except Exception as e:
        print(f"Error getting next ID atomically: {e}")
        raise


def update_indexed_tracking(bucket: str, dataset: str, ids: List[int]):
    key = f"tracking/indexed_ids_{dataset}.json"
    try:
        existing = s3.get_object(Bucket=bucket, Key=key)
        import orjson
        indexed_set = set(orjson.loads(existing["Body"].read()))
    except:
        indexed_set = set()

    indexed_set.update(ids)

    import orjson
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=orjson.dumps(list(indexed_set)),
        ContentType="application/json"
    )

    return ids


def get_files_for_centroid(dataset: str, centroid_id: int, table) -> List[Dict]:
    files = []
    done = False
    start_key = None

    while not done:
        kwargs = {"KeyConditionExpression": "centroid_id = :cid"}
        kwargs["ExpressionAttributeValues"] = {":cid": f"{dataset}#{centroid_id}"}
        if start_key:
            kwargs["ExclusiveStartKey"] = start_key

        response = table.query(**kwargs)
        files.extend(response.get("Items", []))
        start_key = response.get("LastEvaluatedKey")
        done = start_key is None

    return files
