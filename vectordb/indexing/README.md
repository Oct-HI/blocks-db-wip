# indexing — Indexing Pipeline Orchestration

| File | Description |
|------|-------------|
| `indexator.py` | `initialize_database()` — drives the full indexing pipeline via Lithops: preprocess → distribute → build index |

Called from `ServerlessVectorDB.indexing()` which is called from `VectorDBClient.index_dataset()`.
