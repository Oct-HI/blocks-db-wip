# Infra: Blocks-DB Infrastructure

This directory contains the files needed to provision infrastructure in AWS manually.

---

## Main Files

| File | Description |
|------|-------------|
| `setup.py` | Setup and deployment functions |
| `__init__.py` | Public exports |

---

## Not Used (kept for reference)

- `cloudformation.py` - Functions for CloudFormation (not used)
- `cloudformation/blocks-db-infra.yaml` - CloudFormation template (not used)

**Resources created:**
- ECR repository for Lithops runtime
- Lambda execution role
- DynamoDB table (optional)

---

## Lambda

### `lambda/lambda_code.py`

Code for the auto-indexing Lambda.

**Responsibilities:**
1. Detect new files in `pending/`
2. Read vector CSVs
3. Accumulate in DynamoDB
4. When threshold is exceeded, re-index

**Environment variables:**
- `DYNAMODB_TABLE`: Tracking table
- `STORAGE_BUCKET`: S3 bucket
- `THRESHOLD_SIZE_BYTES`: Max size before re-indexing
- `INDEX_IMPLEMENTATION`: "blocks"
- `STARTING_INDEX`: Starting index

---

## Dockerfiles

### `Dockerfile.lambda`

Dockerfile for the Lithops runtime (uses Python 3.10).

**Contents:**
- Base `python:3.10-slim`
- FAISS, numpy, lithops, boto3, etc.
- Configured for `lithops runtime build`

### `Dockerfile.faiss-layer`

Dockerfile to build Lambda layer with FAISS.

**Note:** Uses `numpy<2` — numpy 2.x is incompatible with FAISS.

---

## Lithops

### `lithops-infra.yml`

Lithops configuration (generated automatically during setup).

Location: `~/.lithops/config`

---

## Configuration

### Default values (`config.py`)

```python
DEFAULT_INFRA_CONFIG = {
    "stack_name": "blocksdb-stack-default",
    "lambda_function_name": "blocksdb-autoindexer-default",
    "dynamodb_table_name": "BlocksDB-default",
    "runtime_name": "blocks-db-runtime",
    "layer_name": "blocksdb-layer-faiss-default",
    "lambda_role_name": "blocksdb-autoindexer-role-default",
    "lambda_runtime": "python3.12",
    "lambda_memory_mb": 10240,
    "lambda_timeout_seconds": 900,
    "threshold_size_bytes": 5242880,  # 5 MB
}
```

---

## AWS Structure

When running `blocks-db setup`, the following is created:

### S3

```
bucket/
├── pending/          # Upload CSVs here for auto-indexing
├── inputs/          # Temporary data
├── indexes/          # FAISS index
│   └── {dataset}/
│       └── blocks/
│           └── {num_index}/
└── vectors_{dataset}.csv
```

### DynamoDB

Table: `BlocksDB-default`

**Schema:**
- Partition key: `centroid_id` (String)
- Sort key: `sk` (String)

**Items:**
- `centroid_id: "GLOBAL"`, `sk: "META"` — global metadata
- `centroid_id: "{id}"`, `sk: "DATA"` — vector data

### Lambda

- **Function**: `blocksdb-autoindexer-default`
- **Layer**: `blocksdb-layer-faiss-default` (FAISS + dependencies)
- **Trigger**: S3 `pending/*.csv`

### ECR

- **Repository**: `blocks-db-runtime`
- **Image**: `blocks-db-runtime:latest`

---

## Auto-Indexing Flow

```
1. User runs: blocks-db put dataset vectors.csv
2. Client uploads CSV to: s3://bucket/pending/{timestamp}.csv
3. S3 Trigger fires Lambda
4. Lambda:
   a. Reads CSV from pending/
   b. Accumulates vectors in DynamoDB
   c. If accumulated_size > threshold:
      - Re-indexes all vectors
      - Updates centroid_*.ann in S3
      - Resets accumulator
5. Query: search looks in indexes/ + pending/
```

---

## Notes

- Default threshold is 5MB — adjust based on your vector sizes
- Lambda uses Python 3.12 runtime (not 3.10, for layer compatibility)
- The layer includes FAISS-cpu and numpy<2
- Lithops runtime is built in ECR and used for indexing/distributed queries