# Blocks-DB: Serverless Vector Database

Blocks-DB is a modular serverless vector database built on Lithops and AWS Lambda. It supports block-based indexing with FAISS, distributed querying, and a simple CLI or Python client interface.

---

## Requirements

- **Python 3.10** (venv use recommended)
- **Docker** & **Docker Hub account**
- **AWS account** with access to: S3, Lambda, DynamoDB, ECR, IAM
- **Existing S3 bucket** for storing vectors and indexes

---

## Installation

```bash
# Clone the repository
git clone https://github.com/Oct-HI/blocks-db-wip
cd blocks-db

# (Optional) Create virtual environment
python -m venv venv
source venv/bin/activate

# Install the package
pip install .
```

---

## Initial Setup

### 1. Configure AWS credentials

> **Recommended:** Configure AWS credentials using `~/.aws/credentials` file:
> ```bash
> aws configure
> ```

This is the recommended way. Blocks-DB will automatically read credentials from your AWS config.

Alternatively, you can set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
export AWS_DEFAULT_REGION=us-east-1
```

### 2. Save default bucket and region

```bash
blocks-db configure --bucket your-s3-bucket --region us-east-1
```

This saves configuration to `~/.blocks-db-config/backend_config.json`.

---

## Quick Start

### Setup: Create infrastructure

```bash
blocks-db setup --bucket your-s3-bucket
```

Creates:
- Lambda Layer with FAISS
- Lambda function for auto-indexing
- DynamoDB table for index tracking
- S3 triggers for auto-indexing
- Lithops runtime in ECR

**Customize names:**

```bash
blocks-db setup --bucket your-s3-bucket \
  --stack-name mi-stack \
  --runtime-name mi-runtime \
  --function-name mi-autoindexer \
  --table-name mi-tabla \
  --layer-name mi-layer
```

> **Warning:** Ensure these resources do not already exist, or the setup will fail or update existing resources.

### Initialize database

```bash
blocks-db initialize-database mydataset vectors.csv --config config.json --workers 16
```

This requires an **index config file**. Example (`config.json`):

```json
{
  "features": 96,
  "num_vectors": -1,
  "k_search": 10,
  "k_result": 10,
  "skip_init": false,
  "skip_kmeans": false,
  "kmeans_version": "unbalanced",
  "implementation": "blocks",

  "replication": 1.0,
  "num_index": 16,
  "num_centroids_search": 4,
  "k": 512,
  "n_probe": 32,
  "query_batch_size": 4,

  "index_mem": 10240,
  "search_map_mem": 8192,
  "search_reduce_mem": 2048
}
```

**Parameters:**

| Parameter | Description |
|-----------|-------------|
| `features` | Vector dimensionality |
| `implementation` | "blocks" (default) — block-based indexing |
| `num_index` | Number of index blocks (default: 16) |
| `k` | FAISS IVF k (default: 512) |
| `n_probe` | FAISS IVF n_probe (default: 32) |
| `index_mem` | Index Lambda memory in MB (default: 10240) |
| `search_map_mem` | Search map Lambda memory in MB (default: 8192) |
| `search_reduce_mem` | Search reduce Lambda memory in MB (default: 2048) |
| `replication` | Replication factor |
| `num_vectors` | Total vectors in dataset (-1 = auto-detect) |
| `k_search` | K for search calculation |
| `k_result` | K for final results |
| `query_batch_size` | Query batch size |
| `num_centroids_search` | Centroids to search |
| `skip_init` | Skip initialization |
| `skip_kmeans` | Skip k-means clustering |
| `kmeans_version` | K-means version |

To skip auto-update threshold:
```bash
blocks-db initialize-database mydataset vectors.csv --config config.json --no-update-threshold
```

After initializing, the threshold is automatically configured based on the initial config (num_index, features) and estimated vector size. This threshold controls the size of each block for auto-indexing — the system tries to get as close as possible to this size.

### Add more vectors

```bash
blocks-db put mydataset new_vectors.csv
```

Vectors are stored as "pending" and included in searches automatically.

### Update threshold (after initialize-database)

If you used `--no-update-threshold` or want to adjust manually:

```bash
# Manual value
blocks-db update-threshold 10485760

# Auto-calculate from dataset config
blocks-db update-threshold --dataset mydataset --bucket your-bucket
```

### Query

```bash
# Single vector
blocks-db query mydataset --vector "0.1 0.2 0.3 ..."

# From CSV file
blocks-db query mydataset --file queries.csv --k 10
```

By default searches in index + pending. For index-only search:
```bash
blocks-db query mydataset --file queries.csv --indexed-only
```

### Status

```bash
blocks-db status mydataset

# With details
blocks-db status mydataset -v
```

---

## Commands Reference

| Command | Description | Usage |
|---------|-------------|-------|
| `setup` | Create infrastructure (Lambda, DynamoDB, S3 triggers) | `blocks-db setup --bucket <bucket>` |
| `configure` | Save default bucket and region | `blocks-db configure --bucket <bucket> --region us-east-1` |
| `refresh-credentials` | Refresh AWS credentials in Lithops config | `blocks-db refresh-credentials` |
| `update-threshold` | Update auto-indexer block size threshold | `blocks-db update-threshold [bytes] --dataset <name>` |
| `initialize-database` | Upload dataset and build initial index | `blocks-db initialize-database <name> <csv> --config <json>` |
| `put` | Add vectors to pending storage | `blocks-db put <name> <csv>` |
| `query` | Search vectors (indexed + pending by default) | `blocks-db query <name> --file <csv> --k 10` |
| `status` | Show dataset status and index info | `blocks-db status <name> [-v]` |
| `get` | Retrieve vectors by ID, list vectors, or show pending | `blocks-db get <name> <id>... [--limit N] [--pending]` |

### `get` command usage

```bash
# Get specific vectors by ID
blocks-db get mydataset 1 2 3

# List first N vectors from dataset
blocks-db get mydataset --limit 100

# Show pending vectors
blocks-db get mydataset --pending
```

---

## File Formats

### Vectors CSV

First column: ID (integer), rest: space-separated values.

```
1 0.1 0.2 0.3 ...
2 0.4 0.5 0.6 ...
```

---

## S3 Structure

```
your-bucket/
├── vectors_<dataset>.csv    # Main dataset
├── pending/                 # Pending vectors
├── processed/               # Already processed
│   └── ...
├── indexes/<dataset>/<impl>/
│   ├─config.json
│   └── centroid_*.ann      # Index blocks
└── inputs/                 # Temporary data
```

---

## Python Client

```python
from blocks_db.client import VectorDBClient

client = VectorDBClient(bucket="your-bucket", region="us-east-1")

# Query
vector = [0.1, 0.2, ...]
results, times = client.query("mydataset", vector)
```

---

## AWS Permissions

Blocks-DB needs the following permissions (created automatically by `setup`):

- **S3**: read/write in your bucket
- **Lambda**: create functions, layers
- **DynamoDB**: create table and write
- **ECR**: push images
- **IAM**: roles for Lambda

---

## Notes

- Auto-indexer Lambda triggers when you upload CSV to `pending/` in S3
- Default search is hybrid (index + pending)
- Use `--indexed-only` to search only the existing index
