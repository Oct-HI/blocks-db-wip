# indexing — Indexing Pipeline Orchestration

The initial index build via Lithops (`initialize-database`). Pipeline stages:

1. **Preprocess** (`Preprocessor` ABC): optionally chunk the source CSV into smaller pieces for parallel ingestion.
2. **Distribute** (`Partitioner` ABC): partition vector data across workers.
3. **Build** (`IndexBuilder` ABC): each Lithops worker builds a FAISS IVF index for assigned centroids using `index_factory(features, "IVF{k},Flat")`.
4. **Post-process** (client-side): write per-centroid tag files, DDB centroid tag records, CSV blocks for fast ID lookup, initialize DDB auto-indexer state and ID tracker, optionally update Lambda threshold.

| File | Description |
|------|-------------|
| `indexator.py` | `initialize_database()` — drives the full indexing pipeline via Lithops |

Called from `ServerlessVectorDB.indexing()` which is called from `VectorDBClient.index_dataset()`.

See also: `vectordb/core/README.md` for the ABC contracts.
