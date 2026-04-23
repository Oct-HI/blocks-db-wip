import faiss
import numpy as np

def get_mult_true_neighbours(query_vectors, vectors, ids, features, k):
    """Get the true neighbours for a query vector using a Flat Index"""
    index = faiss.IndexFlatL2(features)
    index2 = faiss.IndexIDMap(index)
    index2.add_with_ids(np.array(vectors), ids)
    
    results = []
    for query_vector in query_vectors:
        _, true_values = index2.search(np.array([query_vector]), k)
        results.append(true_values)
    return results

def get_true_neighbours(query_vector, vectors, ids, features, k):
    """Get the true neighbours for a query vector using a Flat Index"""
    index = faiss.IndexFlatL2(features)
    index2 = faiss.IndexIDMap(index)
    index2.add_with_ids(np.array(vectors), ids)
    _, true_values = index2.search(np.array([query_vector]), k)
    return true_values

def get_ivf_neighbours(vectors, ids, features, num_centroids, n_probe, prompt, k):
    """Get the neighbours for a query vector using an IVF Index"""
    index = faiss.index_factory(features, f"IVF{num_centroids},Flat")
    index.nprobe = n_probe
    index.train(vectors)
    index.add_with_ids(np.array(vectors), ids)
    _, ivf_values = index.search(prompt, k)
    return ivf_values

def calculate_recall(true_values, ann_values):
    """Calculate the recall given a ground truth and an approximation"""
    count = 0
    for num in true_values:
        if num in ann_values:
            count += 1
    percentage = (count / len(true_values)) * 100
    return percentage

def calculate_mult_recall(true_values, ann_values):
    """Calculate the recall given a ground truth and an approximation"""
    
    i = 0
    
    results = []
    for true_value in true_values:
        ann_value = ann_values[i]
        count = 0
        for num in true_value:
            if num in ann_value:
                count += 1
                
        results.append((count / len(true_value)) * 100)
        
        i += 1
        
    return results