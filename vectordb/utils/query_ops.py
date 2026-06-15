import json
from .s3_client import s3


def get_vectors_by_id(bucket, dataset_name, ids):
    """
    Get vectors by ID from 3 sources: CSV, processed, pending.

    Returns dict of {id: result} where result is:
      - {"vector": [...], "source": "indexed|processed"}  if single match
      - [{"vector": [...], "source": "pending", "file": "..."}, ...]  if duplicates in pending
    """
    results = {}
    ids_set = set(ids)

    blocks = _load_csv_blocks(bucket, dataset_name)
    if blocks:
        for vid in ids:
            block = _find_block_for_id(blocks, vid)
            if block:
                vec = _get_vector_by_range(bucket, dataset_name, block, vid)
                if vec is not None:
                    results[vid] = {"vector": vec, "source": "indexed"}
                    ids_set.discard(vid)
    else:
        csv_results = _get_from_full_csv(bucket, dataset_name, ids)
        for vid, vec in csv_results.items():
            results[vid] = {"vector": vec, "source": "indexed"}
        ids_set -= set(csv_results.keys())

    if not ids_set:
        return results

    processed_results = _get_from_processed(bucket, dataset_name, ids_set)
    for vid, entry in processed_results.items():
        results[vid] = entry
        ids_set.discard(vid)

    if not ids_set:
        return results

    pending_results = _get_from_pending(bucket, dataset_name, ids_set)
    for vid, entries in pending_results.items():
        results[vid] = entries
        ids_set.discard(vid)

    return results


def _load_csv_blocks(bucket, dataset_name):
    key = f"csv_blocks_{dataset_name}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def _find_block_for_id(blocks, vid):
    lo, hi = 0, len(blocks) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if blocks[mid]["start_id"] <= vid <= blocks[mid]["end_id"]:
            return blocks[mid]
        elif vid < blocks[mid]["start_id"]:
            hi = mid - 1
        else:
            lo = mid + 1
    return None


def _get_vector_by_range(bucket, dataset_name, block, vid):
    key = f"vectors_{dataset_name}.csv"
    range_header = f"bytes={block['offset']}-{block['offset'] + block['size'] - 1}"
    obj = s3.get_object(Bucket=bucket, Key=key, Range=range_header)
    for raw_line in obj["Body"].iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode()
        id_str, vec_str = line.split(",", 1)
        if int(id_str) == vid:
            return list(map(float, vec_str.split()))
    return None


def _get_from_full_csv(bucket, dataset_name, ids):
    key = f"vectors_{dataset_name}.csv"
    ids_set = set(ids)
    max_id = max(ids_set)
    results = {}

    obj = s3.get_object(Bucket=bucket, Key=key)
    for raw_line in obj["Body"].iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode()
        id_str, vec_str = line.split(",", 1)
        current_id = int(id_str)
        if current_id > max_id:
            break
        if current_id in ids_set:
            results[current_id] = list(map(float, vec_str.split()))
            if len(results) == len(ids_set):
                break
    return results


def _get_from_processed(bucket, dataset_name, ids):
    results = {}
    prefix = f"processed/{dataset_name}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        if not obj["Key"].endswith(".csv"):
            continue
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"]
        for raw_line in body.iter_lines():
            if not raw_line or raw_line.decode().startswith("id,"):
                continue
            id_str, vec_str = raw_line.decode().split(",", 1)
            vid = int(id_str)
            if vid in ids and vid not in results:
                results[vid] = {"vector": list(map(float, vec_str.split())), "source": "processed"}
                if set(results.keys()) == ids:
                    return results
    return results


def _get_from_pending(bucket, dataset_name, ids):
    results = {}
    prefix = f"pending/{dataset_name}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        if not obj["Key"].endswith(".csv"):
            continue
        body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"]
        for raw_line in body.iter_lines():
            if not raw_line or raw_line.decode().startswith("id,"):
                continue
            id_str, vec_str = raw_line.decode().split(",", 1)
            vid = int(id_str)
            if vid in ids:
                entry = {"vector": list(map(float, vec_str.split())), "source": "pending", "file": obj["Key"]}
                if vid in results:
                    results[vid].append(entry)
                else:
                    results[vid] = [entry]
    return results


def list_vectors(bucket, dataset_name, limit=100):
    key = f"vectors_{dataset_name}.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    results = {}

    for raw_line in obj["Body"].iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode()
        id_str, vec_str = line.split(",", 1)
        results[int(id_str)] = list(map(float, vec_str.split()))
        if len(results) >= limit:
            break

    return results


def list_vectors_paginated(bucket, dataset_name, start=0, limit=100):
    key = f"vectors_{dataset_name}.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    results = {}
    current_index = 0

    for raw_line in obj["Body"].iter_lines():
        if not raw_line:
            continue
        if current_index >= start:
            line = raw_line.decode()
            id_str, vec_str = line.split(",", 1)
            results[int(id_str)] = list(map(float, vec_str.split()))
            if len(results) >= limit:
                break
        current_index += 1

    return results
