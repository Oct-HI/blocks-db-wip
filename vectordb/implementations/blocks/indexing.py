from vectordb.core.indexing import IndexBuilder
import faiss
import numpy as np


class FaissIVFIndex(IndexBuilder):

    def __init__(self, params):

        self.features = params.features
        self.k = params.k
        self.nprobe = params.n_probe


    def build(self, ids, vectors):

        index = faiss.index_factory(self.features, f"IVF{self.k},Flat")

        x = np.array(vectors)

        index.train(x)

        index.nprobe = self.nprobe

        index.add_with_ids(x, np.array(ids))

        return index


IMPLEMENTATION_INDEX_BUILDER = FaissIVFIndex