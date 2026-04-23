import faiss
import time
from lithops import Storage

from vectordb.implementations.blocks.indexing import FaissIVFIndex
from vectordb.implementations.blocks.partitioning import BlockPartitioner


def generate_index_blocks(id, obj, params, n_blocks, storage: Storage):

    start = time.time()

    csv_data = obj.data_stream.read().decode("utf-8")

    partitioner = BlockPartitioner(n_blocks)

    blocks = partitioner.partition(csv_data)

    index_builder = FaissIVFIndex(params)

    key_id = id * n_blocks

    for ids, vectors in blocks:

        index = index_builder.build(ids, vectors)

        faiss.write_index(index, f"/tmp/{key_id}.ann")

        storage.upload_file(
            f"/tmp/{key_id}.ann",
            params.storage_bucket,
            f"indexes/{params.dataset}/{params.implementation}/centroid_{key_id}.ann",
        )

        key_id += 1

    return time.time() - start

def get_index_builder():
    return generate_index_blocks