# vectordb/core/preprocess.py

class Preprocessor:
    def __init__(self, params, storage):
        self.params = params
        self.storage = storage

    def run(self, vectors):
        raise NotImplementedError