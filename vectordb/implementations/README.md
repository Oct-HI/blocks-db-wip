# VectorDB Implementation Guide

This document explains how to implement a new **VectorDB backend** compatible with the indexing and querying pipeline.

An implementation is a module located at:
```
vectordb/implementations/<implementation>/
```

It is defined by the following components:

| File | Required | Description |
|---|---|---|
| `indexing.py` | Y | How indexes are built |
| `initialize.py` | Y | How indexing is distributed |
| `querying.py` | Y | How queries are executed |
| `preprocess.py` | N optional | Local preprocessing before indexing (e.g. K-means) |
| `distribute.py` | N optional | Distributed vector assignment before indexing |

---

## Execution Flow

### Indexing
```
initialize_database()
    │
    ├── Phase 0: preprocess.py (optional, local)
    │       e.g. K-means, centroid generation
    │
    ├── Phase 1: distribute.py (optional, distributed via Lithops)
    │       e.g. assign vectors to centroids, upload per-centroid CSVs
    │
    └── Phase 2: initialize.py (distributed via Lithops)
            if distribute ran  → receives index id batches
            if no distribute   → receives raw CSV chunks from object storage
```

### Querying
```
Orchestrator → querying.py
```

---

## 1. indexing.py

Defines how a single index is built.

### Requirements

- Implement a class inheriting from `IndexBuilder`
- Must implement:
```python
def build(self, ids, vectors):
    ...
```

### Responsibilities

- Receive a set of `ids` and `vectors`
- Build an index
- Return the index object

### Required Export
```python
IMPLEMENTATION_INDEX_BUILDER = YourIndexClass
```

### Example
```python
class FaissIVFIndex(IndexBuilder):

    def __init__(self, params):
        self.features = params.features
        self.k = params.k
        self.nprobe = params.n_probe

    def build(self, ids, vectors):
        index = ...
        return index

IMPLEMENTATION_INDEX_BUILDER = FaissIVFIndex
```

---

## 2. initialize.py

Defines how indexing is executed in parallel.

### Required Function
```python
def get_index_builder():
    return indexing_worker
```

This must return a **module-level function** executed by Lithops.

### Indexing Worker — with distribute phase

If `distribute.py` exists, the worker receives a batch of centroid ids:
```python
def indexing_worker(index_ids, params, storage):
    # index_ids: list of centroid ids to build indexes for
    # reads pre-distributed CSVs from:
    #   centroids/{dataset}/{implementation}/{num_index}/{cid}/centroid_*.csv
    ...
```

### Indexing Worker — without distribute phase

If no `distribute.py`, the worker receives raw CSV chunks from object storage:
```python
def indexing_worker(id, obj, params, n_blocks, storage):
    # obj.data_stream: raw CSV chunk
    # n_blocks: number of index blocks to produce
    ...
```

### Storage Convention

Indexes must be stored as:
```
indexes/{dataset}/{implementation}/{num_index}/centroid_<id>.ann
```

---

## 3. preprocess.py (optional)

Defines a **local** preprocessing step that runs before any distributed phase.
Useful for operations that require the full dataset, such as K-means clustering.

### Requirements

- Implement a class inheriting from `Preprocessor`
- Must implement:
```python
def run(self, filename, num_workers):
    ...
```

### Responsibilities

- Download the dataset locally if needed
- Run preprocessing (e.g. K-means)
- Upload any artifacts needed by later phases to object storage
  (e.g. centroids, labels)

### Required Export
```python
IMPLEMENTATION_PREPROCESSOR = YourPreprocessorClass
```

### Example
```python
from vectordb.core.preprocess import Preprocessor

class CentroidPreprocessor(Preprocessor):

    def run(self, filename, num_workers):
        # download dataset, run K-means, upload centroids + labels
        ...

IMPLEMENTATION_PREPROCESSOR = CentroidPreprocessor
```

### Important

This runs **locally**, not in Lambda. It can be a class with state.
If this file does not exist, the preprocessing phase is skipped.

---

## 4. distribute.py (optional)

Defines a **distributed** vector assignment step that runs after preprocessing
and before indexing. Useful for pre-sorting vectors into per-partition buckets
so that each indexing lambda only receives vectors belonging to its partition.

### Requirements

- Must be a **module-level function** (not a class method) — Lithops requirement
- Signature must match Lithops object storage map:
```python
def distribute_vectors(id, obj, params, storage):
    ...
```

### Responsibilities

- Read raw CSV chunk from `obj.data_stream`
- Assign each vector to its partition (e.g. using global labels from preprocess)
- Upload per-partition CSVs to object storage at:
```
centroids/{dataset}/{implementation}/{num_index}/{cid}/centroid_{id}.csv
```

### Required Export
```python
IMPLEMENTATION_DISTRIBUTOR = distribute_vectors
```

### Effect on indexing phase

When `distribute.py` is present and runs successfully, `initialize_database`
automatically switches the indexing phase to pass **index id batches** instead
of raw CSV chunks. Your `initialize.py` worker must handle this accordingly.

### Important

This runs **in Lambda via Lithops**. It must be a plain module-level function —
bound methods cannot be serialized by Lithops. If this file does not exist,
the distribute phase is silently skipped and indexing reads directly from
raw CSV chunks.

---

## 5. querying.py

Defines distributed query execution.

### Query Strategy
```python
class YourQueryStrategy(QueryStrategy):

    def create_map_tasks(self, queries_key, config):
        ...
```

### Responsibilities

- Split queries across index partitions
- Upload task data to object storage
- Return list of tasks:
```python
[
    (task_key, k, config),
    ...
]
```

### Map Function
```python
def map_function(task, k, config, storage):
    ...
```

### Responsibilities

- Load assigned queries and index files
- Execute search
- Return partial results and timing

### Reduce Function
```python
def reduce_function(reduce_key, storage, config):
    ...
```

### Responsibilities

- Merge map outputs
- Deduplicate results
- Select top-k

### Required Exports
```python
IMPLEMENTATION_QUERY_STRATEGY = YourQueryStrategy
```

---

## Integration Points

### Indexing
```python
# Phase 0 — optional local preprocess
module = importlib.import_module(
    f"vectordb.implementations.{implementation}.preprocess"
)
preprocessor = module.IMPLEMENTATION_PREPROCESSOR(params, storage)
preprocessor.run(filename, num_workers)

# Phase 1 — optional distributed distribute
module = importlib.import_module(
    f"vectordb.implementations.{implementation}.distribute"
)
distribute_fn = module.IMPLEMENTATION_DISTRIBUTOR
fexec.map(distribute_fn, vectors_key, ...)

# Phase 2 — required indexing
module = importlib.import_module(
    f"vectordb.implementations.{implementation}.initialize"
)
indexing_function = module.get_index_builder()
fexec.map(indexing_function, ...)
```

### Querying
```python
module = importlib.import_module(
    f"vectordb.implementations.{implementation}.querying"
)

Strategy = module.IMPLEMENTATION_QUERY_STRATEGY
map_fn = module.map_function
reduce_fn = module.reduce_function
```

---

## Constraints

- All worker functions passed to `fexec.map` must be **module-level functions**
- Functions must be **picklable** (Lithops requirement)
- Classes can only be used for local phases (preprocess)
- Use `/tmp` for temporary files
- Clean up temporary files after use

---

## Summary: which files does each implementation need?

| Phase | File | When needed |
|---|---|---|
| Local preprocess | `preprocess.py` | Only if implementation needs a local setup step (e.g. K-means) |
| Distributed distribute | `distribute.py` | Only if vectors must be pre-sorted before indexing |
| Indexing | `initialize.py` | Always required |
| Index builder | `indexing.py` | Always required |
| Querying | `querying.py` | Always required |