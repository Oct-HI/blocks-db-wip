import tempfile
import os

from .s3_client import s3
from .vector_utils import validate_vectors, load_vectors_from_csv
from .index_ops import reindex_after_update


def get_last_id_and_dim(bucket, key):
    obj = s3.get_object(
        Bucket=bucket,
        Key=key,
        Range="bytes=-8192"
    )

    tail = obj["Body"].read().decode()
    lines = [line for line in tail.strip().split("\n") if line.strip()]

    if not lines:
        raise ValueError("CSV empty or corrupted.")

    last_line = lines[-1]

    try:
        id_part, vector_part = last_line.split(",", 1)
    except ValueError:
        raise ValueError("Malformed CSV: missing comma separator.")

    last_id = int(id_part)

    vector_values = [x for x in vector_part.strip().split(" ") if x]
    dim = len(vector_values)

    if dim == 0:
        raise ValueError("Vector dimension detected as 0.")

    return last_id, dim


def upload_dataset(bucket, dataset_name, csv_path):
    key = f"datasets/{dataset_name}/source.csv"

    s3.upload_file(csv_path, bucket, key)

    from .index_ops import delete_indexes
    delete_indexes(bucket, dataset_name)

    print(f"Uploaded {key} and cleared old indexes.")


def delete_dataset(bucket, dataset_name):
    main_key = f"datasets/{dataset_name}/source.csv"
    true_key = f"true_neighbours_{dataset_name}.csv"

    try:
        s3.delete_object(Bucket=bucket, Key=main_key)
    except:
        pass

    try:
        s3.delete_object(Bucket=bucket, Key=true_key)
    except:
        pass

    from .index_ops import delete_indexes
    delete_indexes(bucket, dataset_name)

    print(f"Dataset {dataset_name} and its indexes deleted.")


def update_dataset(bucket, dataset_name, new_vectors, reindex=True):
    key = f"datasets/{dataset_name}/source.csv"

    last_id, expected_dim = get_last_id_and_dim(bucket, key)

    validate_vectors(new_vectors, expected_dim)

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    s3.download_file(bucket, key, tmp_path)

    with open(tmp_path, "a") as f:
        current_id = last_id + 1

        for vec in new_vectors:
            vec_str = " ".join(str(x) for x in vec)
            f.write(f"{current_id},{vec_str}\n")
            current_id += 1

    s3.upload_file(tmp_path, bucket, key)
    os.remove(tmp_path)

    print("Dataset updated successfully.")

    if reindex:
        print("Reindexing after update...")
        reindex_after_update(bucket, dataset_name)

    return True


def update_dataset_from_file(
    bucket,
    dataset_name,
    new_vectors_csv_path,
    reindex=True
):
    key = f"datasets/{dataset_name}/source.csv"

    vectors = load_vectors_from_csv(new_vectors_csv_path)

    last_id, expected_dim = get_last_id_and_dim(bucket, key)
    validate_vectors(vectors, expected_dim)

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    s3.download_file(bucket, key, tmp_path)

    with open(tmp_path, "a") as f:
        current_id = last_id + 1

        for vec in vectors:
            vec_str = " ".join(str(x) for x in vec)
            f.write(f"{current_id},{vec_str}\n")
            current_id += 1

    s3.upload_file(tmp_path, bucket, key)
    os.remove(tmp_path)

    print("Dataset updated successfully.")

    if reindex:
        print("Reindexing...")
        reindex_after_update(bucket, dataset_name)

    return True

def delete_vectors_from_dataset(
    bucket: str,
    dataset_name: str,
    vector_ids,
    reindex: bool = True,
):
    """
    Delete one or multiple vectors by ID from the dataset CSV.

    - Keeps remaining IDs unchanged (no renumbering)
    - Rewrites CSV
    - Optionally triggers full reindex (default=True)
    """

    # Normalize input to a set of ints
    if isinstance(vector_ids, int):
        ids_to_delete = {vector_ids}
    else:
        ids_to_delete = {int(v) for v in vector_ids}

    if not ids_to_delete:
        raise ValueError("No vector IDs provided for deletion.")

    key = f"datasets/{dataset_name}/source.csv"

    # Download dataset locally
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp_path = tmp.name

    s3.download_file(bucket, key, tmp_path)

    kept_lines = []
    deleted_count = 0

    # Read + filter
    with open(tmp_path, "r") as f:
        for line_number, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            try:
                id_part, vector_part = line.split(",", 1)
                vec_id = int(id_part)
            except Exception:
                os.remove(tmp_path)
                raise ValueError(
                    f"Malformed CSV line at {line_number}"
                )

            if vec_id in ids_to_delete:
                deleted_count += 1
                continue

            kept_lines.append(f"{vec_id},{vector_part}\n")

    if deleted_count == 0:
        os.remove(tmp_path)
        print("No matching IDs found. Nothing deleted.")
        return False

    # Rewrite file with remaining vectors
    with open(tmp_path, "w") as f:
        f.writelines(kept_lines)

    # Upload updated CSV
    s3.upload_file(tmp_path, bucket, key)
    os.remove(tmp_path)

    print(f"Deleted {deleted_count} vectors.")

    # Optional full rebuild
    if reindex:
        print("Reindexing dataset after deletion...")
        reindex_after_update(bucket, dataset_name)

    return True

