# vectordb — Blocks-DB Python Package

## Data Flow

```
CLI (cli.py)
  └─ VectorDBClient (client.py)
       └─ ServerlessVectorDB (serverless_vectordb.py)
            ├─ Indexing: indexator.py (Lithops map/reduce on centroids)
            └─ Search:   Orchestrator (Lithops map/reduce)
```

`cli.py` parses CLI args and delegates to `VectorDBClient`.
`client.py` manages datasets (put, status, delete) and wraps both indexing (`index_dataset`) and search (`query`).
`serverless_vectordb.py` bridges client calls to the Lithops `FunctionExecutor` — it handles serialization, function dispatch, and result collection.
`indexing/indexator.py` and `orchestration/orchestrator.py` contain the actual map/reduce logic.

## Top-Level Modules

| File | Description |
|------|-------------|
| `cli.py` | CLI entry point (`blocks-db` command) |
| `client.py` | `VectorDBClient` — Python API for all operations |
| `config.py` | `InfraConfig` and `SvlessVectorDBParams` dataclasses |
| `serverless_vectordb.py` | `ServerlessVectorDB` — Lithops-based index/search wrapper |
| `benchmarks.py` | Recall calculation helpers |

## Submodules

| Directory | Description |
|-----------|-------------|
| `config/` | Default index config (`indexconfig.json`) |
| `core/` | Abstract base classes for backend implementations |
| `implementations/` | Backend implementations (e.g. `blocks`) |
| `indexing/` | Indexing pipeline orchestration |
| `orchestration/` | Distributed map/reduce search |
| `infra/` | AWS infrastructure provisioning |
| `utils/` | S3, DynamoDB, CSV, hybrid search utilities |
