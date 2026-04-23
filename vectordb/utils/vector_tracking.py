import boto3
import orjson
from datetime import datetime
from typing import List, Tuple, Set, Optional
from boto3.dynamodb.conditions import Key
import tempfile
import os
import csv
import io
import time

from .s3_client import s3


class VectorIndexTracker:
    DYNAMODB_TABLE_NAME = "BlocksDB-default"

    def __init__(self, bucket: str, region: str = None):
        self.bucket = bucket
        if region:
            self.dynamodb = boto3.resource("dynamodb", region_name=region)
            self.s3 = boto3.client("s3", region_name=region)
        else:
            self.dynamodb = boto3.resource("dynamodb")
            self.s3 = boto3.client("s3")
        self.table = self.dynamodb.Table(self.DYNAMODB_TABLE_NAME)

    def get_indexed_files_key(self, dataset_name: str) -> str:
        return f"indexed_files_{dataset_name}.json"

    def get_indexed_files(self, dataset_name: str) -> Set[str]:
        indexed_key = self.get_indexed_files_key(dataset_name)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=indexed_key)
            return set(orjson.loads(obj["Body"].read()))
        except self.s3.exceptions.NoSuchKey:
            return set()

    def get_pending_prefix(self, dataset_name: str) -> str:
        return f"pending/{dataset_name}/"

    def get_pending_file_key(self, dataset_name: str, file_id: str = None) -> str:
        if file_id is None:
            file_id = str(int(time.time() * 1000))
        return f"pending/{dataset_name}/{file_id}.csv"

    def get_indexed_ids_key(self, dataset_name: str) -> str:
        return f"indexed_ids_{dataset_name}.json"

    def put_vectors(self, dataset_name: str, vectors: List[Tuple[int, List[float]]], create_file: bool = True) -> str:
        if not vectors:
            return None
        file_id = str(int(time.time() * 1000))
        key = self.get_pending_file_key(dataset_name, file_id)
        
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow(["id", "vector"])
        for vec_id, vec in vectors:
            vec_str = " ".join(str(x) for x in vec)
            writer.writerow([vec_id, vec_str])
        csv_bytes = csv_buffer.getvalue().encode("utf-8")

        s3.put_object(Bucket=self.bucket, Key=key, Body=csv_bytes)
        
        new_ids = [v[0] for v in vectors]
        self._update_pending_tracking(dataset_name, new_ids, key)
        
        return key

    def put_vector(self, dataset_name: str, vector_id: int, vector: List[float]) -> str:
        return self.put_vectors(dataset_name, [(vector_id, vector)])

    def _update_pending_tracking(self, dataset_name: str, vector_ids: List[int], file_key: str):
        try:
            self.table.put_item(Item={
                "centroid_id": "PENDING",
                "sk": f"FILE#{file_key}",
                "dataset": dataset_name,
                "file_key": file_key,
                "vector_ids": vector_ids[:1000] if vector_ids else [],
                "timestamp": int(time.time())
            })
        except Exception as e:
            print(f"DynamoDB tracking error (non-fatal): {e}")

    def get_pending_file_ids(self, dataset_name: str) -> List[str]:
        try:
            response = self.table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('centroid_id').eq('PENDING')
            )
            return [item['file_key'] for item in response.get('Items', []) if item.get('dataset') == dataset_name]
        except Exception:
            return []

    def get_pending_tracking_items(self, dataset_name: str) -> List[dict]:
        try:
            response = self.table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('centroid_id').eq('PENDING')
            )
            return [item for item in response.get('Items', []) if item.get('dataset') == dataset_name]
        except Exception:
            return []

    def clear_pending_file_tracking(self, dataset_name: str):
        items = self.get_pending_tracking_items(dataset_name)
        for item in items:
            try:
                self.table.delete_item(Key={"centroid_id": "PENDING", "sk": f"FILE#{item['file_key']}"})
            except:
                pass

    def get_pending_files(self, dataset_name: str) -> List[str]:
        prefix = self.get_pending_prefix(dataset_name)
        indexed_files = self.get_indexed_files(dataset_name)
        try:
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            if "Contents" not in response:
                return []
            files = []
            for obj in response["Contents"]:
                key = obj["Key"]
                if key.endswith(".csv") and key != prefix and obj.get("Size", 0) > 0 and key not in indexed_files:
                    files.append(key)
            return files
        except Exception:
            return []

    def has_pending_vectors(self, dataset_name: str) -> bool:
        files = self.get_pending_files(dataset_name)
        return len(files) > 0

    def get_pending_vectors(self, dataset_name: str) -> List[Tuple[int, List[float]]]:
        vectors = []
        files = self.get_pending_files(dataset_name)
        for key in files:
            try:
                obj = self.s3.get_object(Bucket=self.bucket, Key=key)
                for raw_line in obj["Body"].iter_lines():
                    if not raw_line:
                        continue
                    line = raw_line.decode()
                    if line.startswith("id,"):
                        continue
                    parts = line.split(",", 1)
                    if len(parts) == 2:
                        try:
                            vec_id = int(parts[0])
                            vec = [float(x) for x in parts[1].strip().split() if x]
                            vectors.append((vec_id, vec))
                        except ValueError:
                            continue
            except Exception as e:
                print(f"Error reading {key}: {e}")
        return vectors

    def create_indexed_tracking(self, dataset_name: str, indexed_ids: List[int]):
        indexed_key = self.get_indexed_ids_key(dataset_name)
        self.s3.put_object(Bucket=self.bucket, Key=indexed_key, Body=orjson.dumps(indexed_ids))

    def mark_vectors_indexed(self, dataset_name: str, indexed_ids: List[int]):
        existing = self.get_indexed_ids(dataset_name)
        existing.update(indexed_ids)
        self.create_indexed_tracking(dataset_name, list(existing))

    def get_indexed_ids(self, dataset_name: str) -> Set[int]:
        indexed_key = self.get_indexed_ids_key(dataset_name)
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=indexed_key)
            return set(orjson.loads(obj["Body"].read()))
        except self.s3.exceptions.NoSuchKey:
            return set()

    def is_indexed(self, dataset_name: str, vector_id: int) -> bool:
        indexed_ids = self.get_indexed_ids(dataset_name)
        return vector_id in indexed_ids

    def clear_pending(self, dataset_name: str):
        files = self.get_pending_files(dataset_name)
        if files:
            objects = [{"Key": k} for k in files]
            self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
        self.clear_pending_file_tracking(dataset_name)

    def delete_tracking(self, dataset_name: str):
        files = self.get_pending_files(dataset_name)
        if files:
            objects = [{"Key": k} for k in files]
            self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": objects})
        self.clear_pending_file_tracking(dataset_name)
        indexed_key = self.get_indexed_ids_key(dataset_name)
        indexed_files_key = self.get_indexed_files_key(dataset_name)
        try:
            self.s3.delete_object(Bucket=self.bucket, Key=indexed_key)
            self.s3.delete_object(Bucket=self.bucket, Key=indexed_files_key)
        except:
            pass