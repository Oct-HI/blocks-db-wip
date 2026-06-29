import faiss
import json
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

    for ids, vectors, tags_dict in blocks:

        index = index_builder.build(ids, vectors)

        faiss.write_index(index, f"/tmp/{key_id}.ann")

        storage.upload_file(
            f"/tmp/{key_id}.ann",
            params.storage_bucket,
            f"indexes/{params.dataset}/{params.implementation}/centroid_{key_id}.ann",
        )

        if tags_dict:
            tags_key = f"indexes/{params.dataset}/{params.implementation}/centroid_{key_id}_tags.json"
            storage.put_object(
                params.storage_bucket,
                tags_key,
                json.dumps(tags_dict).encode("utf-8")
            )

            reverse = {}
            for vid_str, vt in tags_dict.items():
                for k, v in vt.items():
                    reverse.setdefault(f"{k}:{v}", []).append(int(vid_str))
            reverse_key = f"indexes/{params.dataset}/{params.implementation}/centroid_{key_id}_reverse_tags.json"
            storage.put_object(
                params.storage_bucket,
                reverse_key,
                json.dumps(reverse).encode("utf-8")
            )

        key_id += 1

    return time.time() - start

def get_index_builder():
    return generate_index_blocks