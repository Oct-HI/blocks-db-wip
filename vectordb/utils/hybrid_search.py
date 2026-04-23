import numpy as np
from typing import List, Tuple
import faiss


def brute_force_search(
    query_vectors: np.ndarray,
    vectors: List[Tuple[int, List[float]]],
    k: int
) -> List[List[Tuple[int, float]]]:
    """
    Perform brute-force nearest neighbor search on a list of vectors.
    
    Args:
        query_vectors: Query vectors (N x D)
        vectors: List of (id, vector) tuples
        k: Number of nearest neighbors to return
        
    Returns:
        List of lists, each containing (id, distance) tuples sorted by distance
    """
    if not vectors:
        return [[] for _ in range(len(query_vectors))]
    
    ids = np.array([v[0] for v in vectors])
    vec_array = np.array([v[1] for v in vectors]).astype("float32")
    
    index = faiss.IndexFlatL2(vec_array.shape[1])
    index.add(vec_array)
    
    distances, indices = index.search(query_vectors.astype("float32"), min(k, len(vectors)))
    
    results = []
    for i in range(len(query_vectors)):
        result = [(int(ids[idx]), float(distances[i, j])) 
                  for j, idx in enumerate(indices[i]) 
                  if idx != -1]
        results.append(result)
    
    return results


def merge_search_results(
    indexed_results: List[List[Tuple[int, float]]],
    unindexed_results: List[List[Tuple[int, float]]],
    k: int
) -> List[List[Tuple[int, float]]]:
    """
    Merge results from indexed and unindexed searches.
    
    Args:
        indexed_results: Results from FAISS index search
        unindexed_results: Results from brute-force search on unindexed vectors
        k: Number of results to return per query
        
    Returns:
        Merged and deduplicated results sorted by distance
    """
    merged = []
    
    for indexed_res, unindexed_res in zip(indexed_results, unindexed_results):
        combined = indexed_res + unindexed_res
        
        seen = set()
        best = []
        for vid, dist in sorted(combined, key=lambda x: x[1]):
            if vid not in seen:
                best.append((vid, dist))
                seen.add(vid)
                if len(best) >= k:
                    break
        
        merged.append(best)
    
    return merged
