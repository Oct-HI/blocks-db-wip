from lithops import FunctionExecutor, Storage
import time
import importlib
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import orjson

from vectordb.config import SvlessVectorDBParams


class Orchestrator:

    def __init__(self, config: SvlessVectorDBParams, window_time=60):
        self.window_time = window_time
        self.function_executor = FunctionExecutor()
        self.config = config
        self.pool = ThreadPoolExecutor(max_workers=20)

        module = importlib.import_module(
            f"vectordb.implementations.{config.implementation}.querying"
        )
        StrategyClass = getattr(module, "IMPLEMENTATION_QUERY_STRATEGY")
        self.query_strategy = StrategyClass()

        self.map_fn = module.map_function
        self.reduce_fn = module.reduce_function

    def create_reduce_iterdata(self, payload, k, num_queries):
        start = time.time()
        storage = Storage()
        reduce_keys = []

        reduce_iterdata = defaultdict(list)
        for query_dict in payload:
            for key, value in query_dict.items():
                reduce_iterdata[key] = reduce_iterdata[key] + value
        sorted_reduce_iterdata = dict(sorted(reduce_iterdata.items()))

        queries = []
        i = j = 0
        for key, value in sorted_reduce_iterdata.items():
            queries.append([key, value])
            i += 1
            if i == num_queries:
                rkey = f'reduce/res_{j}.json'
                storage.put_object(bucket=self.config.storage_bucket, key=rkey, body=orjson.dumps({"queries": queries, "k": k}))
                reduce_keys.append(rkey)
                j += 1
                queries = []
                i = 0

        if queries:
            rkey = f'reduce/res_{j}.json'
            storage.put_object(bucket=self.config.storage_bucket, key=rkey, body=orjson.dumps({"queries": queries, "k": k}))
            reduce_keys.append(rkey)

        return reduce_keys, time.time() - start

    def divide_map_results(self, futures_res):
        results, times = [], []
        for res in futures_res:
            results.append(res[0])
            times.append(res[1])
        return results, times

    def divide_reduce_results(self, futures_res):
        results, times = [], []
        for res in futures_res:
            for q_res in res[0]:
                results.append(q_res)
            times.append(res[1])
        return results, times

    def search(self, id_query, queries, n=None, k_search=None, k_result=None):
        n        = n        if n        is not None else len(queries)
        k_search = k_search if k_search is not None else self.config.k_search
        k_result = k_result if k_result is not None else self.config.k_result

        start = init = time.time()

        queries_key = f"queries_{self.config.dataset}_{self.config.num_index}.csv"
        self.function_executor.storage.put_object(
            bucket=self.config.storage_bucket,
            key=queries_key,
            body=orjson.dumps(queries.tolist())
        )

        index_to_compute = self.query_strategy.create_map_tasks(queries_key, self.config)

        create_map_data = time.time()

        if self.function_executor.config["lithops"]["backend"] == "k8s":
            self.function_executor.config["k8s"]["runtime_cpu"]    = self.config.search_map_cpus
            self.function_executor.config["k8s"]["runtime_memory"] = self.config.search_map_mem

        futures = self.function_executor.map(
            self.map_fn,
            index_to_compute,
            runtime_memory=self.config.search_map_mem
        )
        map_futures_res = self.function_executor.get_result(wait_dur_sec=0)
        lambda_invocation_map = [
            f.stats["worker_func_start_tstamp"] - f.stats["host_job_create_tstamp"]
            for f in futures
        ]
        map_execution = time.time()
        map_res, map_times = self.divide_map_results(map_futures_res)

        # --- Reduce ---
        reduce_iterdata, reduce_iterdata_times = self.create_reduce_iterdata(map_res, k_result, 1000)

        if self.function_executor.config["lithops"]["backend"] == "k8s":
            self.function_executor.config["k8s"]["runtime_cpu"]    = self.config.search_reduce_cpus
            self.function_executor.config["k8s"]["runtime_memory"] = self.config.search_reduce_mem

        reduce_iterdata = [(x, self.config) for x in reduce_iterdata]
        create_reduce_data = time.time()

        futures = self.function_executor.map(
            self.reduce_fn,
            reduce_iterdata,
            runtime_memory=self.config.search_reduce_mem
        )
        reduce_futures_res = self.function_executor.get_result(wait_dur_sec=0)
        lambda_invocation_reduce = [
            f.stats["worker_func_start_tstamp"] - f.stats["host_job_create_tstamp"]
            for f in futures
        ]
        reduce_execution = time.time()
        reduce_res, reduce_times = self.divide_reduce_results(reduce_futures_res)
        end = time.time()

        impl = self.config.implementation
        timers = {
            f'{id_query}_create_map_data_{impl}':    create_map_data    - init,
            f'{id_query}_map_{impl}':                map_times,
            f'{id_query}_map_invocation_{impl}':     lambda_invocation_map,
            f'{id_query}_map_execution_{impl}':      map_execution      - create_map_data,
            f'{id_query}_create_reduce_data_{impl}': create_reduce_data - map_execution,
            f'{id_query}_reduce_iterdata_{impl}':    reduce_iterdata_times,
            f'{id_query}_reduce_{impl}':             reduce_times,
            f'{id_query}_reduce_invocation_{impl}':  lambda_invocation_reduce,
            f'{id_query}_reduce_execution_{impl}':   reduce_execution   - create_reduce_data,
            f'{id_query}_divide_reduce_{impl}':      end                - reduce_execution,
            f'{id_query}_total_querying_{impl}':     end                - start,
        }

        return reduce_res, timers