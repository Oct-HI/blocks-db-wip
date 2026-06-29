# core — Abstract Base Classes

Contracts that every backend implementation must satisfy.

| File | Class | Purpose |
|------|-------|---------|
| `indexing.py` | `IndexBuilder` | Build a FAISS index from vectors |
| `partitioning.py` | `Partitioner` | Split data into blocks |
| `preprocess.py` | `Preprocessor` | Optional pre-indexing step |
| `querying.py` | `QueryStrategy` | Create map tasks for distributed search |

Each backend under `implementations/` provides concrete subclasses and module-level entry points (`IMPLEMENTATION_QUERY_STRATEGY`, etc.).
