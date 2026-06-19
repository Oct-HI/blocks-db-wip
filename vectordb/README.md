# vectordb — Blocks-DB Python Package

Top-level modules:

| File | Description |
|------|-------------|
| `cli.py` | CLI entry point (`blocks-db` command) |
| `client.py` | `VectorDBClient` — Python API for all operations |
| `config.py` | `InfraConfig` and `SvlessVectorDBParams` dataclasses |
| `serverless_vectordb.py` | `ServerlessVectorDB` — Lithops-based index/search wrapper |
| `benchmarks.py` | Recall calculation helpers |

Submodules:

| Directory | Description |
|-----------|-------------|
| `config/` | Default index config (`indexconfig.json`) |
| `core/` | Abstract base classes for backend implementations |
| `implementations/` | Backend implementations (e.g. `blocks`) |
| `indexing/` | Indexing pipeline orchestration |
| `orchestration/` | Distributed map/reduce search |
| `infra/` | AWS infrastructure provisioning |
| `utils/` | S3, DynamoDB, CSV, hybrid search utilities |
