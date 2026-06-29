# utils — Shared Utilities

| File | Description |
|------|-------------|
| `s3_client.py` | Singleton S3 client (shared across modules) |
| `s3_utils.py` | S3 Express bucket detection helpers |
| `dataset_ops.py` | Upload, delete, update datasets (CSV in S3) |
| `index_ops.py` | Save, load, delete, reindex FAISS index configs |
| `query_ops.py` | `get_vectors_by_id`, `list_vectors`, `list_vectors_paginated` |
| `vector_utils.py` | CSV parsing: load vectors with/without IDs/tags |
| `vector_tracking.py` | `VectorIndexTracker` — DynamoDB + S3 tracking for pending/indexed vectors |
| `hybrid_search.py` | `brute_force_search` + `merge_search_results` for hybrid queries |
