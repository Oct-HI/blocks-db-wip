import faiss
import numpy as np
import orjson
from lithops import Storage
import time
from collections import defaultdict
import os
import csv
import io

from vectordb.core.querying import QueryStrategy


class BlocksQueryStrategy(QueryStrategy):

    def create_map_tasks(self, queries_key, config, storage=None):
        tasks = []
        for i in range(0, config.num_index, config.query_batch_size):
            tasks.append(
                (
                    (queries_key, list(range(i, min(i + config.query_batch_size, config.num_index)))),
                    config.k_search,
                    config
                )
            )

        if storage is not None:
            pending_prefix = f"pending/{config.dataset}/"
            try:
                pending_files = storage.list_keys(config.storage_bucket, pending_prefix)
                pending_csvs = [
                    f for f in pending_files
                    if f.endswith(".csv") and f != pending_prefix
                ]
                if pending_csvs:
                    tasks.append(
                        (
                            ("pending", queries_key, pending_csvs),
                            config.k_search,
                            config
                        )
                    )
            except Exception:
                pass

        return tasks


def _search_indexed(task_spec, k, storage, config, start, source="indexed"):
    queries_key = task_spec
    queries = storage.get_object(bucket=config.storage_bucket, key=queries_key[0])
    queries = orjson.loads(queries)
    queries_json = {
        f'indexes/{config.dataset}/{config.implementation}/centroid_{key}.ann': queries
        for key in queries_key[1]
    }

    res_queries = defaultdict(list)

    for file_idx, (key, queries) in enumerate(queries_json.items()):
        storage.download_file(config.storage_bucket, key, f'/tmp/index_{file_idx}.ann')
        index = faiss.read_index(f'/tmp/index_{file_idx}.ann')
        d, i = index.search(np.array(queries), k)
        for x in range(len(queries)):
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

    all_ids = []
    all_vectors = []

    for pf in pending_files:
        raw = storage.get_object(bucket=config.storage_bucket, key=pf)
        if isinstance(raw, bytes):
            raw = raw.decode()
        reader = csv.reader(io.StringIO(raw))
        for row in reader:
            if not row or row[0] == "id":
                continue
            parts = row[0].split(",", 1) if len(row) == 1 else [row[0], ",".join(row[1:])]
            if len(parts) < 2:
                continue
            vid = int(parts[0])
            vec = [float(x) for x in parts[1].strip().split() if x]
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