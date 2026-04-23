import faiss
import numpy as np
import orjson
from lithops import Storage
import time
from collections import defaultdict
import os

from vectordb.core.querying import QueryStrategy


class BlocksQueryStrategy(QueryStrategy):

    def create_map_tasks(self, queries_key, config):
        tasks = []
        for i in range(0, config.num_index, config.query_batch_size):
            tasks.append(
                (
                    (queries_key, list(range(i, min(i + config.query_batch_size, config.num_index)))),
                    config.k_search,
                    config
                )
            )
        return tasks

def map_function(queries_key, k, storage: Storage, config):
    """Lithops map worker"""
    start = time.time()
    faiss.omp_set_num_threads(6)

    res_queries = defaultdict(list)
    start_d_queries = end_d_queries = time.time()

    if config.implementation == "blocks":
        start_d_queries = time.time()
        queries = storage.get_object(bucket=config.storage_bucket, key=queries_key[0])
        queries = orjson.loads(queries)
        queries_json = {
            f'indexes/{config.dataset}/{config.implementation}/centroid_{key}.ann': queries
            for key in queries_key[1]
        }
        end_d_queries = time.time()

    all_index = []
    all_index_memory = []
    all_a_queries = []

    for file_idx, (key, queries) in enumerate(queries_json.items()):
        t0 = time.time()
        storage.download_file(config.storage_bucket, key, f'/tmp/index_{file_idx}.ann')
        all_index.append(time.time() - t0)

    for file_idx, (key, queries) in enumerate(queries_json.items()):
        t0 = time.time()
        index = faiss.read_index(f'/tmp/index_{file_idx}.ann')
        all_index_memory.append(time.time() - t0)

        t0 = time.time()
        if config.implementation == "blocks":
            d, i = index.search(np.array(queries), k)
            for x in range(len(queries)):
                res_queries[x].append([d[x].tolist(), i[x].tolist()])
        all_a_queries.append(time.time() - t0)

        os.remove(f'/tmp/index_{file_idx}.ann')

    start_reduce = time.time()
    final_results = {}
    for key, res in res_queries.items():
        concat_res = []
        for dists, ids in res:
            for dist, id in zip(dists, ids):
                concat_res.append([id, dist])
        seen = set()
        best_vectors = []
        for id, dist in sorted(concat_res, key=lambda x: x[1]):
            if id not in seen:
                best_vectors.append([id, dist])
                seen.add(id)
        final_results[key] = best_vectors[:k]

    end = time.time()
    return final_results, [
        end - start,
        end_d_queries - start_d_queries,
        all_index,
        all_index_memory,
        all_a_queries,
        end - start_reduce,
    ]


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
        for id, dist in sorted(res[1], key=lambda x: x[1]):
            if id not in seen:
                best_vectors.append((id, dist))
                seen.add(id)
        final_results.append(best_vectors[:k])

    return final_results, time.time() - start


IMPLEMENTATION_QUERY_STRATEGY = BlocksQueryStrategy