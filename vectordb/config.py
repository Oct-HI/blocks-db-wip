from dataclasses import dataclass, field
from typing import Optional

@dataclass
class InfraConfig:
    lambda_function_name: str = "blocksdb-autoindexer-default"
    lambda_runtime: str = "python3.12"
    lambda_memory_mb: int = 10240
    lambda_timeout_seconds: int = 900
    lambda_role_name: str = "blocksdb-autoindexer-role-default"
    dynamodb_table_name: str = "BlocksDB-default"
    layer_name: str = "blocksdb-layer-faiss-default"
    threshold_size_bytes: int = 5242880  # 5 MB
    runtime_name: str = "blocks-db-runtime"
    sqs_use_sqs: bool = False
    sqs_queue_name: str = "blocksdb-pending-default"
    sqs_queue_url: str = None
    sqs_batch_size: int = 10
    sqs_batch_window: int = 0


DEFAULT_INFRA_CONFIG = InfraConfig()


def get_infra_config(overrides: dict = None) -> dict:
    config = DEFAULT_INFRA_CONFIG.__dict__.copy()
    if overrides:
        config.update(overrides)
    return config


@dataclass
class SvlessVectorDBParams:
    
    # General arguments
    dataset: str = "glove"
    features: int = 64
    num_vectors: int = -1
    k_search: int = 5
    k_result: int = 5
    skip_init: bool = False
    skip_kmeans: bool = False
    kmeans_version: str = "unbalanced"
    implementation: str = "blocks"
    
    # Custom algorithm arguments
    replication: int = 1
    num_index: int = 4
    num_centroids_search: int = 4
    k: int = 4096
    n_probe: int = 1024
    query_batch_size: int = 16,
    
    # Storage
    storage_bucket: str = None
    centroids_key: str = "centroids.json"
    labels_key: str = "labels.json"
    
    # Runtime
    index_mem: int = 8192
    search_map_cpus: int = 6
    search_map_mem: int = 9216
    search_reduce_cpus: int = 1
    search_reduce_mem: int = 2048
    
    # Hybrid search settings
    auto_index_threshold_mb: Optional[int] = None
    dynamodb_table_name: str = "BlocksDB-default"
    
    # Tag filtering (per-query, not persisted)
    filter_tags: dict = None

    # Post-filter overfetch multiplier (k * overfetch candidates before tag filter)
    post_filter_overfetch: int = 2

    # Tag filter mode: 'post' (overfetch + loop) or 'pre' (reverse-index + IDSelectorBatch)
    filter_mode: str = "post"

    # DynamoDB connection info (injected at query time)
    dynamodb_region: str = None

    # Extra fields from index config
    total_vectors: int = -1
    bytes_per_vector: int = 776
    bytes_per_block: int = 5242880
    csv_block_size: int = 500000
