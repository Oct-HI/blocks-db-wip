import json
import os
import boto3
import numpy as np
import csv
from typing import List, Tuple

from .utils.dataset_ops import (
    upload_dataset,
    delete_dataset,
)

from .utils.query_ops import (
    get_vectors_by_id,
    list_vectors,
    list_vectors_paginated,
)

from .utils.index_ops import (
    list_indexes,
    save_index_config,
    load_index_config,
    delete_indexes,
)

from .utils.vector_tracking import VectorIndexTracker
from .utils.hybrid_search import brute_force_search, merge_search_results

from .serverless_vectordb import ServerlessVectorDB
from .infra import refresh_lithops_credentials

class VectorDBClient:

    def __init__(self, bucket: str, region: str = None):
        self.bucket = bucket

        if region:
            self.s3 = boto3.client("s3", region_name=region)
        else:
            self.s3 = boto3.client("s3")
        
        self.tracker = VectorIndexTracker(bucket, region)

    def create_dataset(self, name: str, csv_path: str):
        """
        Upload a local CSV file as a new dataset.
        Renames automatically to vectors_{name}.csv
        """

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found.")

        upload_dataset(self.bucket, name, csv_path)

        print(f"Dataset '{name}' created successfully.")

    def delete_dataset(self, name: str):
        delete_dataset(self.bucket, name)
        self.tracker.delete_tracking(name)
        print(f"Dataset '{name}' deleted.")

    def put_vectors(self, dataset_name: str, vectors: List[Tuple[int, List[float]]], auto_index: bool = False):
        """
        Add new vectors to the pending storage (not yet indexed).
        
        Args:
            dataset_name: Name of the dataset
            vectors: List of (id, vector) tuples
            auto_index: If True and threshold reached, trigger indexing (requires Lambda setup)
            
        Returns:
            Number of vectors added
        """
        count = self.tracker.put_vectors(dataset_name, vectors)
        print(f"Added {count} vectors to pending storage for dataset '{dataset_name}'.")
        return count

    def put_vector(self, dataset_name: str, vector_id: int, vector: List[float]):
        """Add a single vector to pending storage."""
        return self.put_vectors(dataset_name, [(vector_id, vector)])

    def get_pending_vectors(self, dataset_name: str) -> List[Tuple[int, List[float]]]:
        """Get all vectors that are pending indexing."""
        return self.tracker.get_pending_vectors(dataset_name)

    def has_pending_vectors(self, dataset_name: str) -> bool:
        """Check if there are pending vectors that need indexing."""
        return self.tracker.has_pending_vectors(dataset_name)

    def is_vector_indexed(self, dataset_name: str, vector_id: int) -> bool:
        """Check if a specific vector is indexed."""
        return self.tracker.is_indexed(dataset_name, vector_id)

    def get_indexed_ids(self, dataset_name: str) -> set:
        """Get all indexed vector IDs."""
        return self.tracker.get_indexed_ids(dataset_name)

    def get_indexed_count(self, dataset_name: str) -> int:
        """Get count of indexed vectors from DynamoDB counter."""
        return self.tracker.get_next_id(dataset_name)

    def mark_vectors_indexed(self, dataset_name: str, indexed_ids: List[int]):
        """Mark vectors as indexed (called after reindex)."""
        self.tracker.mark_vectors_indexed(dataset_name, indexed_ids)

    def refresh_credentials(self):
        return refresh_lithops_credentials()

    def list_datasets(self):
        """
        List all datasets in bucket following naming convention.
        """

        response = self.s3.list_objects_v2(Bucket=self.bucket)

        datasets = []

        if "Contents" not in response:
            return datasets

        for obj in response["Contents"]:
            key = obj["Key"]

            if key.startswith("vectors_") and key.endswith(".csv"):
                name = key.replace("vectors_", "").replace(".csv", "")
                datasets.append(name)

        return datasets

    def get_vectors(self, dataset_name: str, ids):
        return get_vectors_by_id(self.bucket, dataset_name, ids)

    def list_vectors(self, dataset_name: str, limit: int = 100):
        return list_vectors(self.bucket, dataset_name, limit)

    def list_vectors_paginated(self, dataset_name: str, start=0, limit=100):
        return list_vectors_paginated(
            self.bucket,
            dataset_name,
            start,
            limit
        )
    
    def list_indexes(self, dataset_name: str):
        return list_indexes(self.bucket, dataset_name)
    
    def index_dataset(self, dataset_name: str, config: dict, num_workers: int = 16, save_config: bool = True, track_indexed: bool = True):
        """
        Run indexing for a dataset using provided configuration.

        Args:
            dataset_name (str): Name of the dataset to index.
            config (dict): Indexing configuration (must include 'implementation' and 'num_index').
            num_workers (int): Number of workers for parallel indexing.
            save_config (bool): If True, saves the index configuration to S3 after indexing.
            track_indexed (bool): If True, tracks indexed vector IDs for hybrid queries.
        Returns:
            dict: Timing stats from the indexing process.
        """

        config["dataset"] = dataset_name
        config["storage_bucket"] = self.bucket

        print(f"Initializing ServerlessVectorDB for dataset '{dataset_name}'...")
        sv_vectordb = ServerlessVectorDB(**config)

        filename = f"vectors_{dataset_name}.csv"

        print("Starting indexing...")
        total_times = sv_vectordb.indexing(filename, num_workers)
        print("Indexing completed.")

        features = config.get("features", 96)
        num_index = config.get("num_index", 16)
        total_vectors = config.get("num_vectors", -1)
        if total_vectors <= 0:
            total_vectors = self._get_vector_count(dataset_name)
        
        bytes_per_vector = 8 + (features * 8)
        vectors_per_block = total_vectors // num_index
        bytes_per_block = vectors_per_block * bytes_per_vector
        
        print(f"\nAuto-indexer block size info:")
        print(f"  Total vectors: {total_vectors:,}")
        print(f"  Features: {features}")
        print(f"  Bytes per vector: {bytes_per_vector}")
        print(f"  Blocks: {num_index}")
        print(f"  Vectors per block: {vectors_per_block:,}")
        print(f"  Bytes per block: {bytes_per_block:,}")
        print(f"\nTo sync Lambda threshold, run:")
        print(f"  blocks-db update-threshold {bytes_per_block} --dataset {dataset_name} --bucket {self.bucket}")
        print(f"  (or just: blocks-db update-threshold --dataset {dataset_name} --bucket {self.bucket} for auto)")

        if save_config:
            config["total_vectors"] = total_vectors
            config["bytes_per_vector"] = bytes_per_vector
            config["bytes_per_block"] = bytes_per_block
            self.save_index_config(dataset_name, config)
            self._setup_auto_indexer_state(dataset_name, config)

        if track_indexed:
            self._track_indexed_vectors_from_csv(dataset_name, features)

        return total_times
    
    def _get_vector_count(self, dataset_name: str) -> int:
        """Count total vectors in dataset."""
        s3 = boto3.client("s3")
        key = f"vectors_{dataset_name}.csv"
        try:
            response = s3.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return sum(1 for line in content.strip().split('\n') if line.strip())
        except Exception:
            return 100000
    
    def _setup_auto_indexer_state(self, dataset_name: str, config: dict):
        """Set up DynamoDB state for auto-indexer to continue from where manual indexing left off."""
        from pathlib import Path
        
        dynamodb = boto3.resource("dynamodb")
        
        config_path = Path(__file__).parent.parent / "infra" / "infrastructure_config.json"
        if config_path.exists():
            import json
            infra_config = json.loads(config_path.read_text())
            table_name = infra_config.get("dynamodb_table_name", "BlocksDB-default")
        else:
            table_name = "BlocksDB-default"
        
        try:
            table = dynamodb.Table(table_name)
            num_index = config.get("num_index", 16)
            
            table.update_item(
                Key={"centroid_id": "GLOBAL_CONFIG", "sk": "META"},
                UpdateExpression="SET current_accumulated_size = :zero, current_centroid_id = :next",
                ExpressionAttributeValues={":zero": 0, ":next": num_index}
            )
            print(f"Auto-indexer state initialized: next centroid will be {num_index}")
        except Exception as e:
            print(f"Warning: Could not set auto-indexer state: {e}")

    def reindex_pending(self, dataset_name: str, config: dict = None, num_workers: int = 16):
        """
        Reindex pending vectors and merge them into existing indexes.
        
        This rebuilds all indexes with the current pending vectors included.
        Pending vectors are marked as indexed after successful reindex.
        
        Args:
            dataset_name: Name of the dataset
            config: Optional config dict (uses stored config if not provided)
            num_workers: Number of workers for indexing
            
        Returns:
            Timing stats from the indexing process
        """
        if not self.has_pending_vectors(dataset_name):
            print("No pending vectors to reindex.")
            return {}
        
        pending = self.get_pending_vectors(dataset_name)
        print(f"Found {len(pending)} pending vectors.")
        
        if config is None:
            indexes = self.list_indexes(dataset_name)
            if not indexes:
                raise ValueError(f"No index config found for '{dataset_name}'. Provide config explicitly.")
            implementation, num_index = indexes[0]
            config = load_index_config(self.bucket, dataset_name, implementation, num_index)
        
        pending_ids = [v[0] for v in pending]
        
        delete_indexes(self.bucket, dataset_name)
        
        config["dataset"] = dataset_name
        config["storage_bucket"] = self.bucket
        
        sv_vectordb = ServerlessVectorDB(**config)
        filename = f"vectors_{dataset_name}.csv"
        
        print("Rebuilding indexes with pending vectors...")
        total_times = sv_vectordb.indexing(filename, num_workers)
        print("Reindexing completed.")
        
        self.tracker.clear_pending(dataset_name)
        
        all_indexed_ids = list(self.tracker.get_indexed_ids(dataset_name)) + pending_ids
        self.tracker.create_indexed_tracking(dataset_name, all_indexed_ids)
        print(f"Now tracking {len(all_indexed_ids)} total indexed vectors.")
        
        return total_times

    def index_pending_separate(self, dataset_name: str, config: dict = None):
        """
        Create separate indexes for pending vectors without rebuilding main index.
        
        This adds pending vectors as additional data without full reindex.
        They will be searched via hybrid query until a full reindex is done.
        
        Args:
            dataset_name: Name of the dataset
            config: Optional config dict (uses stored config if not provided)
        """
        if not self.has_pending_vectors(dataset_name):
            print("No pending vectors to index.")
            return
        
        if config is None:
            indexes = self.list_indexes(dataset_name)
            if not indexes:
                raise ValueError(f"No index config found for '{dataset_name}'. Provide config explicitly.")
            implementation, num_index = indexes[0]
            config = load_index_config(self.bucket, dataset_name, implementation, num_index)
        
        pending = self.get_pending_vectors(dataset_name)
        pending_ids = [v[0] for v in pending]
        
        self.tracker.mark_vectors_indexed(dataset_name, pending_ids)
        print(f"Marked {len(pending_ids)} pending vectors as indexed for hybrid search.")

    def _track_indexed_vectors_from_csv(self, dataset_name: str, features: int):
        """Read the main CSV and track all vectors as indexed + build csv_blocks for optimized get."""
        key = f"vectors_{dataset_name}.csv"
        indexed_ids = []
        block_size = 500000  # ~500KB per block
        blocks = []

        try:
            head = self.s3.head_object(Bucket=self.bucket, Key=key)
        except Exception as e:
            print(f"Warning: Could not get CSV metadata: {e}")
            return

        try:
            current_offset = 0
            current_block_start_id = None
            current_block_size = 0
            last_vid = None

            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            for raw_line in obj["Body"].iter_lines():
                if not raw_line:
                    continue
                line = raw_line.decode()
                id_str = line.split(",")[0]
                vec_id = int(id_str)
                indexed_ids.append(vec_id)

                if current_block_start_id is None:
                    current_block_start_id = vec_id

                current_block_size += len(raw_line) + 1

                if current_block_size >= block_size:
                    blocks.append({
                        "start_id": current_block_start_id,
                        "end_id": vec_id,
                        "offset": current_offset,
                        "size": current_block_size
                    })
                    current_offset += current_block_size
                    current_block_start_id = None
                    current_block_size = 0

                last_vid = vec_id

            if current_block_start_id is not None and last_vid is not None:
                blocks.append({
                    "start_id": current_block_start_id,
                    "end_id": last_vid,
                    "offset": current_offset,
                    "size": current_block_size
                })

            if blocks:
                blocks_key = f"csv_blocks_{dataset_name}.json"
                self.s3.put_object(Bucket=self.bucket, Key=blocks_key, Body=json.dumps(blocks))
                print(f"Built {len(blocks)} CSV blocks for optimized get.")

            if indexed_ids:
                self.tracker.create_indexed_tracking(dataset_name, indexed_ids)
                # Initialize DynamoDB counter for atomic ID tracking
                next_id = max(indexed_ids) + 1
                self.tracker.initialize_next_id(dataset_name, next_id)
                print(f"Tracked {len(indexed_ids)} vectors as indexed.")
                print(f"Initialized DynamoDB next_id={next_id} for atomic ID tracking.")
        except Exception as e:
            print(f"Warning: Could not track indexed vectors: {e}")

    def save_index_config(self, dataset_name: str, config: dict):
        """
        Save index configuration to S3 for future reindexing.
        """
        save_index_config(self.bucket, dataset_name, config)
        print(f"Index configuration saved for dataset '{dataset_name}'.")


    def delete_index_configs(self, dataset_name: str):
        """
        Delete all saved index configuration files (config.json) for a dataset.
        """
        prefix = f"indexes/{dataset_name}/"
        paginator = self.s3.get_paginator("list_objects_v2")

        deleted_count = 0

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            if "Contents" in page:
                configs_to_delete = [
                    {"Key": obj["Key"]}
                    for obj in page["Contents"]
                    if obj["Key"].endswith("config.json")
                ]
                if configs_to_delete:
                    self.s3.delete_objects(
                        Bucket=self.bucket,
                        Delete={"Objects": configs_to_delete}
                    )
                    deleted_count += len(configs_to_delete)

        print(f"Deleted {deleted_count} config(s) for dataset '{dataset_name}'")
    
    def query(self, dataset_name: str, vector: List[float], k: int = None, hybrid: bool = True):
        """
        Query a single vector. Always searches everything (indexed + pending).
        
        Args:
            dataset_name: Name of the dataset
            vector: Query vector (list of floats)
            k: Number of results (defaults to config k_result)
            hybrid: If True, include pending vectors in search (default: True)
            
        Returns:
            List of (id, distance) tuples sorted by distance
        """
        results, times = self.query_batch(dataset_name, [vector], k=k, hybrid=hybrid)
        return results[0], times

    def query_batch(self, dataset_name: str, vectors: List[List[float]], k: int = None, hybrid: bool = True):
        """
        Query multiple vectors. Always searches everything (indexed + pending) by default.
        
        Args:
            dataset_name: Name of the dataset
            vectors: List of query vectors
            k: Number of results per query
            hybrid: If True, include pending vectors in search (default: True)
            
        Returns:
            neighbours, times
        """

        if not vectors:
            raise ValueError("No query vectors provided.")

        vectors_np = np.array(vectors)
        
        if hybrid:
            return self._query_hybrid(dataset_name, vectors_np, k)
        else:
            return self._query_indexed_only(dataset_name, vectors_np, k)
    
    def query_indexed_only(self, dataset_name: str, vector: List[float] = None, vectors: List[List[float]] = None, k: int = None):
        """
        Query only the indexed vectors (no pending).
        
        Args:
            dataset_name: Name of the dataset
            vector: Single query vector
            vectors: Multiple query vectors (alternative to single vector)
            k: Number of results
            
        Returns:
            Search results from indexed data only
        """
        if vector is not None:
            vecs = [vector]
        elif vectors is not None:
            vecs = vectors
        else:
            raise ValueError("Provide either vector or vectors")
        
        return self._query_indexed_only(dataset_name, np.array(vecs), k)

    def _query_indexed_only(self, dataset_name: str, vectors_np: np.ndarray, k: int = None):
        """Query only the FAISS index (no pending vectors)."""
        k = k if k is not None else self._get_k_result(dataset_name)
        
        try:
            sv_vectordb = self._load_default_index(dataset_name)
            neighbours, times = sv_vectordb.search(0, vectors_np)
        except ValueError as e:
            print(f"No index available: {e}")
            neighbours = [[] for _ in range(len(vectors_np))]
            times = {"error": str(e)}
        
        return neighbours, times

    def _query_hybrid(self, dataset_name: str, vectors_np: np.ndarray, k: int = None):
        """Internal hybrid query implementation."""
        k = k if k is not None else self._get_k_result(dataset_name)
        
        indexed_results = None
        indexed_times = None
        
        try:
            sv_vectordb = self._load_default_index(dataset_name)
            indexed_results, indexed_times = sv_vectordb.search(0, vectors_np)
        except ValueError as e:
            print(f"No index available: {e}")
            indexed_results = [[] for _ in range(len(vectors_np))]
            indexed_times = {"error": str(e)}
        
        unindexed_results = None
        
        if self.has_pending_vectors(dataset_name):
            unindexed = self.get_pending_vectors(dataset_name)
            if unindexed:
                unindexed_results = brute_force_search(vectors_np, unindexed, k)
        
        if unindexed_results is None:
            merged = indexed_results
        else:
            merged = merge_search_results(indexed_results, unindexed_results, k)
        
        times = indexed_times if indexed_times else {}
        times["hybrid_search"] = True
        times["has_unindexed"] = unindexed_results is not None
        
        return merged, times
    
    def query_from_file(self, dataset_name: str, csv_path: str, hybrid: bool = True, k: int = None):
        """
        Query ALL vectors from a CSV file. Always searches everything by default.
        
        Args:
            dataset_name: Name of the dataset
            csv_path: Path to CSV file with query vectors (space-separated floats per row)
            hybrid: If True, include pending vectors (default: True)
            k: Number of results per query
            
        Returns:
            neighbours, times
        """

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"{csv_path} not found.")

        vectors = []

        with open(csv_path, "r") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                vec = [float(x) for x in row[0].split(" ") if x]
                vectors.append(vec)

        if not vectors:
            raise ValueError("No valid vectors found in CSV.")

        vectors_np = np.array(vectors)
        
        if hybrid:
            return self._query_hybrid(dataset_name, vectors_np, k)
        else:
            return self._query_indexed_only(dataset_name, vectors_np, k)

    def query_hybrid(self, dataset_name: str, vectors: List[List[float]], k: int = None):
        """
        Explicit hybrid query - searches both indexed and pending vectors.
        Alias for query_batch with hybrid=True.
        
        Args:
            dataset_name: Name of the dataset
            vectors: List of query vectors
            k: Number of results per query (defaults to config k_result)
            
        Returns:
            Merged search results with (id, distance) tuples
        """
        return self.query_batch(dataset_name, vectors, k=k, hybrid=True)

    def _query_hybrid(self, dataset_name: str, vectors_np: np.ndarray, k: int = None):
        """Internal hybrid query implementation."""
        k = k if k is not None else self._get_k_result(dataset_name)
        
        indexed_results = None
        indexed_times = None
        
        try:
            sv_vectordb = self._load_default_index(dataset_name)
            indexed_results, indexed_times = sv_vectordb.search(0, vectors_np)
        except ValueError as e:
            print(f"No index available: {e}")
            indexed_results = [[] for _ in range(len(vectors_np))]
            indexed_times = {}
        
        unindexed_results = None
        unindexed_times = {}
        
        if self.has_pending_vectors(dataset_name):
            unindexed = self.get_pending_vectors(dataset_name)
            if unindexed:
                unindexed_results = brute_force_search(vectors_np, unindexed, k)
        
        if unindexed_results is None:
            merged = indexed_results
        else:
            merged = merge_search_results(indexed_results, unindexed_results, k)
        
        times = indexed_times if indexed_times else {}
        times["hybrid_search"] = True
        times["has_unindexed"] = unindexed_results is not None
        
        return merged, times

    def _get_k_result(self, dataset_name: str) -> int:
        """Get k_result from index config."""
        try:
            config = self._load_index_config_for_search(dataset_name)
            return config.get("k_result", 10)
        except:
            return 10

    def _load_index_config_for_search(self, dataset_name: str) -> dict:
        """Load index config for search operations."""
        indexes = self.list_indexes(dataset_name)
        if not indexes:
            raise ValueError(f"No index found for dataset '{dataset_name}'.")
        
        if len(indexes) > 1:
            raise ValueError(
                f"Multiple indexes found for dataset '{dataset_name}'. "
                f"Specify implementation and num_index manually."
            )
        
        implementation, num_index = indexes[0]
        return load_index_config(self.bucket, dataset_name, implementation, num_index)
    
    def _load_default_index(self, dataset_name: str):
        """
        Load the only stored index config for a dataset.
        Raises error if none or more than one exist.
        """

        indexes = self.list_indexes(dataset_name)

        if not indexes:
            raise ValueError(f"No index found for dataset '{dataset_name}'. Run indexing first.")

        if len(indexes) > 1:
            raise ValueError(
                f"Multiple indexes found for dataset '{dataset_name}'. "
                f"Specify implementation and num_index manually."
            )

        implementation, num_index = indexes[0]

        config = load_index_config(
            self.bucket,
            dataset_name,
            implementation,
            num_index
        )

        config["dataset"] = dataset_name
        config["storage_bucket"] = self.bucket

        return ServerlessVectorDB(**config)