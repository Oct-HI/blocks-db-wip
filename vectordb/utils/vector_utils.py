import json
from typing import List, Tuple, Optional


def validate_vectors(vectors, expected_dim):
    for i, vec in enumerate(vectors):
        if len(vec) != expected_dim:
            raise ValueError(
                f"Vector {i} has dimension {len(vec)}, expected {expected_dim}"
            )


def load_vectors_from_csv(csv_path: str) -> List[List[float]]:
    """
    Load vectors from a CSV file formatted as:
    id,val1 val2 val3 ...
    
    Returns:
        List of vectors (without IDs)
    """
    vectors = []

    with open(csv_path, "r") as f:
        for line_number, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            try:
                _, vector_part = line.split(",", 1)
            except ValueError:
                raise ValueError(
                    f"Malformed line {line_number}: missing comma separator."
                )

            values = [
                float(x) for x in vector_part.strip().split(" ") if x
            ]

            vectors.append(values)

    if not vectors:
        raise ValueError("No valid vectors found in file.")

    return vectors


def load_vectors_with_ids_from_csv(csv_path: str) -> List[Tuple[int, List[float]]]:
    """
    Load vectors from a CSV file formatted as:
    id,val1 val2 val3 ...
    
    Returns:
        List of (id, vector) tuples
    """
    vectors = []

    with open(csv_path, "r") as f:
        for line_number, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            try:
                id_str, vector_part = line.split(",", 1)
                vec_id = int(id_str)
            except ValueError:
                raise ValueError(
                    f"Malformed line {line_number}: missing comma or invalid ID."
                )

            values = [
                float(x) for x in vector_part.strip().split(" ") if x
            ]

            vectors.append((vec_id, values))

    if not vectors:
        raise ValueError("No valid vectors found in file.")

    return vectors


def load_vectors_with_ids_and_tags_from_csv(csv_path: str) -> List[Tuple[int, List[float], Optional[dict]]]:
    """
    Load vectors with per-row tags from a CSV file.

    CSV formats accepted:
      id,val1 val2 val3...
      id,val1 val2 val3...,{"source":"web"}

    If no 3rd column, tags is None.
    Returns:
        List of (id, vector, tags_or_None) tuples
    """
    import csv as csv_mod
    vectors = []

    with open(csv_path, "r", newline="") as f:
        reader = csv_mod.reader(f)
        for row in reader:
            if not row or not row[0].strip():
                continue
            try:
                vec_id = int(row[0])
            except (ValueError, IndexError):
                continue
            if len(row) < 2:
                continue
            vec = [float(x) for x in row[1].strip().split() if x]
            if not vec:
                continue
            tags = None
            if len(row) > 2 and row[2].strip():
                try:
                    tags = json.loads(row[2])
                    if not isinstance(tags, dict):
                        tags = None
                except (json.JSONDecodeError, ValueError):
                    tags = None
            vectors.append((vec_id, vec, tags))

    return vectors