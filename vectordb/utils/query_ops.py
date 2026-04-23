from .s3_client import s3


def get_vectors_by_id(bucket, dataset_name, ids):
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
            vector = list(map(float, vec_str.split()))
            results[current_id] = vector
            if len(results) == len(ids_set):
                break

    return results


def list_vectors(bucket, dataset_name, limit=100):
    """
    Return up to `limit` vectors as {id: vector}.
    Avoids downloading the entire CSV for large datasets.
    """
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
    """
    Return `limit` vectors starting at row offset `start` as {id: vector}.
    """
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