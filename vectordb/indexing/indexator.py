import time
import logging
import importlib


def initialize_database(filename, params, fexec, num_workers=16):

    init = time.time()

    if not params.skip_kmeans:
        try:
            module = importlib.import_module(
                f"vectordb.implementations.{params.implementation}.preprocess"
            )
            preprocessor_cls = module.IMPLEMENTATION_PREPROCESSOR
            preprocessor = preprocessor_cls(params, fexec.storage)
            preprocessor.run(filename, num_workers)
        except ModuleNotFoundError:
            logging.info(f"No preprocess step for implementation '{params.implementation}'")

    vectors_key = f"{params.storage_bucket}/{filename}"

    distribute_time = None
    try:
        dist_module = importlib.import_module(
            f"vectordb.implementations.{params.implementation}.distribute"
        )
        distribute_fn = dist_module.IMPLEMENTATION_DISTRIBUTOR

        logging.info("Starting distribute phase")
        futures = fexec.map(
            distribute_fn,
            vectors_key,
            extra_args=[params],
            obj_chunk_number=num_workers,
            runtime_memory=params.index_mem
        )
        distribute_time = fexec.get_result()
        logging.info("Distribute phase complete")

    except ModuleNotFoundError:
        logging.info(f"No distribute step for implementation '{params.implementation}'")

    logging.info("Starting indexing")

    index_module = importlib.import_module(
        f"vectordb.implementations.{params.implementation}.initialize"
    )
    indexing_function = index_module.get_index_builder()

    if distribute_time is not None:
        all_index = list(range(params.num_index))
        n_per_worker = max(1, params.num_index // num_workers)
        index_batches = [
            all_index[i:i + n_per_worker]
            for i in range(0, len(all_index), n_per_worker)
        ]
        futures = fexec.map(
            indexing_function,
            index_batches,
            extra_args=[params],
            runtime_memory=params.index_mem,
        )
    else:
        obj_chunk = num_workers
        n_blocks_per_function = max(1, int(params.num_index / obj_chunk))
        futures = fexec.map(
            indexing_function,
            vectors_key,
            extra_args=[params, n_blocks_per_function],
            obj_chunk_number=obj_chunk,
            runtime_memory=params.index_mem,
        )

    indexing_function_time = fexec.get_result()

    lambda_invocation_indexing = [
        f.stats["worker_func_start_tstamp"] - f.stats["host_job_create_tstamp"]
        for f in futures
    ]

    end = time.time()

    timers = {
        f"distribute_{params.implementation}": distribute_time,
        f"indexing_function_{params.implementation}": indexing_function_time,
        f"indexing_function_invocation_{params.implementation}": lambda_invocation_indexing,
        f"total_indexing_{params.implementation}": end - init,
    }

    return timers