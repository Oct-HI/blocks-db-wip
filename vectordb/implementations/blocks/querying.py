import faiss
import numpy as np
import orjson
import boto3
from lithops import Storage
import time
import json
from collections import defaultdict
import os
import csv
import io

from vectordb.core.querying import QueryStrategy


def _centroid_tags_match(centroid_tags, filter_tags):
    """Check if a centroid's aggregated tags match ALL filter key-value pairs.
    
    Centroid tags are stored as {key: [values...]} (list of possible values).
    A filter {k: v} matches if v is in centroid_tags[k].
    """
    if not filter_tags:
        return True
    if not centroid_tags:
        return False
    for k, v in filter_tags.items():
        values = centroid_tags.get(k)
        if not values:
            return False
        if isinstance(values, list):
            if v not in values:
                return False
        elif isinstance(values, str):
            if values != v:
                return False
        else:
            if values != v:
                return False
    return True


def _get_matching_centroids(table, dataset, num_index, filter_tags):
    """Query DynamoDB for centroids whose aggregated tags match the filter.
    Returns list of centroid IDs (ints) that match.

    Centroids without a DDB tag record are always included (backward compat
    for datasets with no tags). Centroids with a DDB record are filtered.
    """
    if not filter_tags:
        return list(range(num_index))

    centroids_with_ddb = {}

    try:
        response = table.query(
            KeyConditionExpression=(
                boto3.dynamodb.conditions.Key('centroid_id').eq(f"DATASET#{dataset}") &
                boto3.dynamodb.conditions.Key('sk').begins_with('CENTROID#')
            )
        )
        items = response.get("Items", [])
    except Exception as e:
        print(f"DynamoDB query failed (falling back to all centroids): {e}")
        return list(range(num_index))

    for item in items:
        cid = int(item["sk"].split("#")[1])
        centroids_with_ddb[cid] = item.get("tags")

    matching = []
    for cid in range(num_index):
        if cid not in centroids_with_ddb:
            matching.append(cid)
        else:
            tags = centroids_with_ddb[cid]
            if not tags or _centroid_tags_match(tags, filter_tags):
                matching.append(cid)

    return matching


def _get_matching_pending_files(table, dataset, filter_tags):
    """Query DynamoDB for pending files whose tags match the filter.
    Returns list of file keys that match, or None if no filter (all pending).
    Returns empty list if filter set but no matches or query fails.
    """
    if not filter_tags:
        return None  # None means "all pending"

    matching = []
    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('centroid_id').eq('PENDING')
        )
        items = response.get("Items", [])
    except Exception:
        return []  # If DynamoDB fails, don't search any pending files

    for item in items:
        if item.get("dataset") != dataset:
            continue
        raw_tags = item.get("tags")
        if not raw_tags:
            continue
        if isinstance(raw_tags, str):
            tags = json.loads(raw_tags)
        else:
            tags = raw_tags
        if _centroid_tags_match(tags, filter_tags):
            matching.append(item["file_key"])

    return matching


class BlocksQueryStrategy(QueryStrategy):

    def create_map_tasks(self, queries_key, config, storage=None, filter_tags=None):
        tasks = []

        if filter_tags:
            table_name = getattr(config, 'dynamodb_table_name', "BlocksDB-default")
            region = getattr(config, 'dynamodb_region', None)
            try:
                if region:
                    dynamodb = boto3.resource("dynamodb", region_name=region)
                else:
                    dynamodb = boto3.resource("dynamodb")
                table = dynamodb.Table(table_name)
            except Exception:
                table = None

            if table:
                centroid_ids = _get_matching_centroids(table, config.dataset, config.num_index, filter_tags)
                pending_csvs = _get_matching_pending_files(table, config.dataset, filter_tags)
            else:
                centroid_ids = list(range(config.num_index))
                pending_csvs = None
        else:
            centroid_ids = list(range(config.num_index))
            pending_csvs = None

        for i in range(0, len(centroid_ids), config.query_batch_size):
            batch = centroid_ids[i:i + config.query_batch_size]
            tasks.append(
                (
                    (queries_key, batch),
                    config.k_search,
                    config
                )
            )

        if pending_csvs is None:
            pending_csvs = []
            if storage is not None:
                pending_prefix = f"pending/{config.dataset}/"
                try:
                    pending_files = storage.list_keys(config.storage_bucket, pending_prefix)
                    pending_csvs = [
                        f for f in pending_files
                        if f.endswith(".csv") and f != pending_prefix
                    ]
                except Exception:
                    pass

        if pending_csvs:
            tasks.append(
                (
                    ("pending", queries_key, pending_csvs),
                    config.k_search,
                    config
                )
            )

        return tasks


def _search_indexed(task_spec, k, storage, config, start, source="indexed"):
    queries_key = task_spec
    queries = storage.get_object(bucket=config.storage_bucket, key=queries_key[0])
    queries = orjson.loads(queries)
    queries_json = {
        f'indexes/{config.dataset}/{config.implementation}/centroid_{key}.ann': queries
        for key in queries_key[1]
    }

    filter_tags = getattr(config, 'filter_tags', None)
    overfetch = getattr(config, 'post_filter_overfetch', 3)
    search_k = k * overfetch if filter_tags else k

    res_queries = defaultdict(list)

    for file_idx, (key, queries) in enumerate(queries_json.items()):
        storage.download_file(config.storage_bucket, key, f'/tmp/index_{file_idx}.ann')
        index = faiss.read_index(f'/tmp/index_{file_idx}.ann')

        centroid_tags = {}
        if filter_tags:
            cid = queries_key[1][file_idx] if file_idx < len(queries_key[1]) else None
            if cid is not None:
                tags_key = f'indexes/{config.dataset}/{config.implementation}/centroid_{cid}_tags.json'
                try:
                    raw = storage.get_object(bucket=config.storage_bucket, key=tags_key)
                    if isinstance(raw, bytes):
                        raw = raw.decode()
                    centroid_tags = json.loads(raw)
                except Exception:
                    pass

        d, i = index.search(np.array(queries), search_k)
        for x in range(len(queries)):
            if filter_tags and centroid_tags:
                fd, fi = [], []
                for dist, idx in zip(d[x], i[x]):
                    vt = centroid_tags.get(str(int(idx)))
                    if vt is not None:
                        if all(vt.get(tk) == tv for tk, tv in filter_tags.items()):
                            fd.append(float(dist))
                            fi.append(int(idx))
                    elif not centroid_tags:
                        fd.append(float(dist))
                        fi.append(int(idx))
                res_queries[x].append([fd, fi])
            else:
                res_queries[x].append([d[x].tolist(), i[x].tolist()])
        os.remove(f'/tmp/index_{file_idx}.ann')

    final_results = {}
    for key, res in res_queries.items():
        concat_res = []
        for dists, ids in res:
            for dist, id in zip(dists, ids):
                concat_res.append([id, dist, source])
        seen = set()
        best_vectors = []
        for id, dist, src in sorted(concat_res, key=lambda x: x[1]):
            if id not in seen:
                best_vectors.append([id, dist, src])
                seen.add(id)
        final_results[key] = best_vectors[:k]

    return final_results


def _search_pending(queries_key, pending_files, k, storage, config, start, source="pending"):
    queries = storage.get_object(bucket=config.storage_bucket, key=queries_key)
    queries = orjson.loads(queries)

    filter_tags = getattr(config, 'filter_tags', None)

    all_ids = []
    all_vectors = []

    for pf in pending_files:
        try:
            raw = storage.get_object(bucket=config.storage_bucket, key=pf)
        except Exception:
            continue
        if isinstance(raw, bytes):
            raw = raw.decode()

        has_any_tags = False
        rows_buffer = []
        reader = csv.reader(io.StringIO(raw))
        for row in reader:
            if not row or row[0] == "id":
                continue
            if len(row) < 2:
                continue
            rows_buffer.append(row)
            if len(row) > 2 and row[2].strip():
                has_any_tags = True

        for row in rows_buffer:
            try:
                vid = int(row[0])
                vec = [float(x) for x in row[1].strip().split() if x]
            except (ValueError, IndexError):
                continue

            tags = None
            if len(row) > 2 and row[2].strip():
                try:
                    tags = json.loads(row[2]) if isinstance(row[2], str) else row[2]
                except (json.JSONDecodeError, ValueError):
                    pass

            if filter_tags:
                if tags is not None:
                    if not all(tags.get(k) == v for k, v in filter_tags.items()):
                        continue
                elif has_any_tags:
                    continue

            all_ids.append(vid)
            all_vectors.append(vec)

    if not all_vectors:
        return {i: [] for i in range(len(queries))}

    vec_array = np.array(all_vectors).astype("float32")
    index = faiss.IndexFlatL2(vec_array.shape[1])
    index.add(vec_array)

    distances, indices = index.search(np.array(queries).astype("float32"), min(k, len(all_vectors)))

    final_results = {}
    for q_idx in range(len(queries)):
        results = []
        for j, idx in enumerate(indices[q_idx]):
            if idx != -1:
                results.append([int(all_ids[idx]), float(distances[q_idx, j]), source])
        final_results[q_idx] = results

    return final_results


def map_function(task_spec, k, storage: Storage, config):
    """Lithops map worker"""
    start = time.time()

    if isinstance(task_spec[0], str) and task_spec[0] == "pending":
        _, queries_key, pending_files = task_spec
        final_results = _search_pending(queries_key, pending_files, k, storage, config, start)
        end = time.time()
        return final_results, [end - start, 0, [], [], [], end - start]

    queries_key = task_spec
    final_results = _search_indexed(queries_key, k, storage, config, start)
    end = time.time()
    return final_results, [end - start, 0, [], [], [], end - start]


def reduce_function(reduce_key, storage: Storage, config):
    """Lithops reduce worker"""
    start = time.time()

    res_json = storage.get_object(bucket=config.storage_bucket, key=reduce_key).decode("UTF-8")
    res_json = orjson.loads(res_json)

    results = res_json["queries"]
    k = res_json["k"]

    final_results = []
    for res in results:
        seen = set()
        best_vectors = []
        for id, dist, source in sorted(res[1], key=lambda x: x[1]):
            if id not in seen:
                best_vectors.append((id, dist, source))
                seen.add(id)
        final_results.append(best_vectors[:k])

    return final_results, time.time() - start


IMPLEMENTATION_QUERY_STRATEGY = BlocksQueryStrategy