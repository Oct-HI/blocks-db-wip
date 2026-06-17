<div align="center">

# Blocks-DB: Serverless Vector Database

<p>
  <img src="https://img.shields.io/badge/🐍Python-3.10-4ecdc4?style=for-the-badge&logo=python&logoColor=white">
  <img src="https://img.shields.io/github/stars/Oct-HI/blocks-db-wip?style=for-the-badge&logo=github&logoColor=white">
  <a href="https://doi.org/10.1145/3769769"><img src="https://img.shields.io/badge/📄Paper-10.1145/3769769-ff6b6b?style=for-the-badge"></a>
</p>

<div align="center" style="margin: 30px 0;">
  <img src="./README.assets/blocks-db-scheme.png" alt="Blocks-DB Scheme" style="max-width: 100%; height: auto;">
</div>

</div>

---

Blocks-DB is a modular serverless vector database built on Lithops and AWS Lambda. It supports block-based indexing with FAISS, distributed querying, and a simple CLI or Python client interface.

---

## 📋 Requirements

- **Python 3.10** (venv use recommended)
- **Docker** & **Docker Hub account**
- **AWS account** with access to: S3, Lambda, DynamoDB, ECR, IAM
- **Existing S3 bucket** for storing vectors and indexes

---

## 📦 Installation

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

## ⚙️ Initial Setup

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

For SQS-based auto-indexer:
```bash
blocks-db configure --bucket your-s3-bucket --region us-east-1 --sqs
```

This saves configuration to `~/.blocks-db-config/backend_config.json`.

---

## 🚦 Auto-Indexer Modes

Blocks-DB supports three auto-indexer configurations, selected at `setup`:

| Mode | `setup` flag | `configure` flag | Use case |
|------|:------------:|:----------------:|----------|
| **S3 Triggers** (default) | *(none)* | *(none)* | Standard S3 buckets with notification support |
| **SQS** | `--sqs` | `--sqs` | Buckets without S3 notification support, or when you prefer queue-based triggers |
| **S3 Express One Zone** | `--s3express` | *(auto-detected)* | S3 Express One Zone buckets (name ends in `--x-s3`). Auto-enables SQS since Express buckets don't support S3 notifications |

### How it works

- **S3 Triggers**: Uploading a CSV to `pending/<dataset>/` fires a Lambda notification directly.
- **SQS**: The CLI sends a message to an SQS queue after each `put`. Lambda polls the queue. Includes a DLQ for failed messages.
- **S3 Express**: Same as SQS, plus adds `s3express:CreateSession` permission to the Lambda role and auto-detects the availability zone from the bucket name.

---

## 🏃 Quick Start

### Setup: Create infrastructure

```bash
# Default — S3 Triggers
blocks-db setup --bucket your-s3-bucket

# SQS mode
blocks-db setup --bucket your-s3-bucket --sqs

# S3 Express One Zone mode (auto-enables SQS)
blocks-db setup --bucket your-bucket--use1-az6--x-s3 --s3express
```

Creates:
- Lambda Layer with FAISS
- Lambda function for auto-indexing
- DynamoDB table for index tracking
- S3 triggers **or** SQS queue (+ DLQ) for auto-indexing
- Lithops runtime in ECR

**Customize names:**

```bash
blocks-db setup --bucket your-s3-bucket \
  --runtime-name mi-runtime \
  --function-name mi-autoindexer \
  --table-name mi-tabla \
  --layer-name mi-layer \
  --role-name mi-rol
```

**Other options:**
```bash
# Custom threshold (bytes) for auto-indexer block size
blocks-db setup --bucket your-bucket --threshold 10485760

# Skip DynamoDB table or Lithops runtime (if already built)
blocks-db setup --bucket your-bucket --skip-vector-table --skip-runtime
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

**Options:**

```bash
# Skip auto-update of Lambda threshold
blocks-db initialize-database mydataset vectors.csv --config config.json --no-update-threshold

# Build csv_blocks from local file (skip S3 re-download during tracking)
blocks-db initialize-database mydataset vectors.csv --config config.json --build-local

# Skip DynamoDB state init and vector tracking (benchmark purity)
blocks-db initialize-database mydataset vectors.csv --config config.json --skip-auto-indexer
```

After initializing, the threshold is automatically configured based on the initial config (num_index, features) and estimated vector size. This threshold controls the size of each block for auto-indexing — the system tries to get as close as possible to this size.

### Add more vectors

```bash
blocks-db put mydataset new_vectors.csv
```

Vectors are stored as "pending" and included in searches automatically.

**With metadata tags:**
```bash
blocks-db put mydataset new_vectors.csv --tags '{"source":"web","category":"news"}'
```

Tags are stored per batch and can be used to filter searches later. Each vector in the batch inherits the same tags.

**Per-vector tags (3rd CSV column):**

Alternatively, each row in the CSV can carry its own tags as a JSON third
column. This is more granular than batch-level `--tags`:

```
1 0.1 0.2 ... {"source":"web","priority":"high"}
2 0.4 0.5 ... {"source":"api","priority":"low"}
```

When `--tags` and per-vector tags are both present, per-vector tags take
precedence.

**Single-vector mode:**
```bash
blocks-db put mydataset single_vector.csv --single
```

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

**Filtered search (requires tags on put):**
```bash
# Only search centroids and pending files matching ALL specified tags
blocks-db query mydataset --file queries.csv --k 10 --filter '{"source":"web"}'
```

**Filter modes:**

| Mode | Flag | Behavior |
|------|------|----------|
| Post-filter (default) | `--filter-mode post` | Overfetches k×2 per centroid, discards non-matching. Fast, default. |
| Pre-filter | `--filter-mode pre` | Uses reverse index per centroid to restrict FAISS search to only matching IDs. May find results post-filter misses. |

Post-filter is the default and performs well for most use cases. Pre-filter
may find more results since it doesn't rely on overfetching, at the cost of
loading a reverse index per centroid.

**Get vectors by tags:**

```bash
blocks-db get-by-tags mydataset --filter '{"priority":"high"}' --limit 20
```

Lists vector IDs whose tags match the filter. Useful for inspection.

**Custom batch size** (number of centroid `.ann` files per map worker):
```bash
blocks-db query mydataset --file queries.csv --batch-size 2
```

### Status

```bash
blocks-db status mydataset

# With details
blocks-db status mydataset -v
```

---

## 📖 Commands Reference

| Command | Description | Usage |
|---------|-------------|-------|
| `setup` | Create infrastructure (Lambda, DynamoDB, SQS/S3 triggers) | `blocks-db setup --bucket <b> [--sqs \| --s3express]` |
| `configure` | Save default bucket and region | `blocks-db configure --bucket <b> --region <r> [--sqs]` |
| `refresh-credentials` | Refresh AWS credentials in Lithops config | `blocks-db refresh-credentials` |
| `update-threshold` | Update auto-indexer block size threshold | `blocks-db update-threshold [bytes] --dataset <name>` |
| `initialize-database` | Upload dataset and build initial index | `blocks-db initialize-database <n> <csv> --config <j> [--build-local]` |
| `put` | Add vectors to pending storage | `blocks-db put <name> <csv> [--tags <json>] [--single]` |
| `query` | Search vectors (indexed + pending by default) | `blocks-db query <n> --file <csv> --k <N> [--filter <j>] [--filter-mode post\|pre] [--batch-size <N>]` |
| `status` | Show dataset status and index info | `blocks-db status <name> [-v]` |
| `get` | Retrieve vectors by ID, list vectors, or show pending | `blocks-db get <n> <id>... [--limit N] [--pending]` |
| `get-by-tags` | List vector IDs matching tags | `blocks-db get-by-tags <n> --filter <json> [--limit N]` |

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

## 📄 File Formats

### Vectors CSV

First column: ID (integer), rest: space-separated values.

```
1 0.1 0.2 0.3 ...
2 0.4 0.5 0.6 ...
```

### Vectors CSV with Per-Vector Tags

Add a third column with a JSON object for per-vector tags:

```
1 0.1 0.2 ... {"source":"web","priority":"high"}
2 0.4 0.5 ... {"source":"api","priority":"low"}
```

When the third column is present, `initialize-database` and the auto-indexer
Lambda store the tags alongside each vector in the index. Vectors without a
third column are untagged and match any filter.

---

## 🗂️ S3 Structure

```
your-bucket/
├── vectors_<dataset>.csv              # Main dataset
├── csv_blocks_<dataset>.json          # Block index for fast ID lookup
├── pending/<dataset>/                 # Pending vectors (individual CSVs)
├── processed/<dataset>/               # Processed pending batches
├── tracking/                          # Vector tracking metadata
├── indexes/<dataset>/blocks/
│   ├── config.json
│   ├── centroid_*.ann                 # FAISS index blocks
│   ├── centroid_*_tags.json           # Per-vector tags (forward index)
│   └── centroid_*_reverse_tags.json   # Reverse index for pre-filter mode
├── datasets/<dataset>/source.csv      # Alternative dataset path
└── inputs/                            # Temporary Lithops data
```

---

## 🐍 Python Client

```python
from blocks_db.client import VectorDBClient

client = VectorDBClient(bucket="your-bucket", region="us-east-1")

# Query
vector = [0.1, 0.2, ...]
results, times = client.query("mydataset", vector)
```

---

## 🔐 AWS Permissions

Blocks-DB needs the following permissions (created automatically by `setup`):

- **S3**: read/write in your bucket
- **Lambda**: create functions, layers
- **DynamoDB**: create table and write
- **ECR**: push images
- **IAM**: roles for Lambda
- **SQS** (if `--sqs` or `--s3express`): create queues, event source mappings
- **S3 Express** (if `--s3express`): `s3express:CreateSession` on the bucket

---

## 📝 Notes

- **Auto-indexer**: In S3 Triggers mode, uploading a CSV to `pending/<dataset>/` fires the Lambda. In SQS mode, the CLI posts a message after each `put`.
- **S3 Express One Zone**: Bucket name must end with `--<az>--x-s3` (auto-detected). Uses SQS since Express buckets don't support S3 notifications.
- **Tag filtering**: Supports two modes. **Post-filter** (default) overfetches k×2 candidates per centroid then discards non-matching. **Pre-filter** (`--filter-mode pre`) uses a reverse index (`_reverse_tags.json`) with FAISS `IDSelectorBatch` to search only matching IDs. Tags can be per-vector (3rd CSV column) or per-batch (`--tags`). Both modes rely on DynamoDB for centroid-level pre-filtering to exclude centroids without matching tags. `_tags.json` and `_reverse_tags.json` are written per centroid during indexing and auto-indexing.
- **Default search** is hybrid (index + pending). Use `--indexed-only` to search only the existing index.
- **Threshold**: Controls the accumulated pending size (in bytes) that triggers the auto-indexer. Auto-calculated from dataset config or set manually.

---

## 📖 Citation

Based on: *Building Stateless Serverless Vector DBs via Block-based Data Partitioning*  
Daniel Barcelona-Pons, Raúl Gracia-Tinedo, Albert Cañadilla-Domingo, Xavier Roca-Canals, Pedro García-López  
Proc. ACM Manag. Data 3, 6 (SIGMOD), Article 304 (December 2025)  
DOI: [10.1145/3769769](https://doi.org/10.1145/3769769)
