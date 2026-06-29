# orchestration — Distributed Map/Reduce Search

Search query execution via Lithops map/reduce:

1. **Upload queries**: serialized as JSON to `queries/testdata/{uuid}_queries_{dataset}_{num_index}.csv` in S3.
2. **Create map tasks** (`BlocksQueryStrategy.create_map_tasks()`): determine which centroids to search (all or DDB-filtered if `filter_tags` set). One task per `query_batch_size` centroids, plus one task for pending files.
3. **Map phase**: each worker downloads assigned `.ann` files from S3, runs FAISS `search()` with pre/post filter, returns per-query result sets.
4. **Reduce preparation** (`create_reduce_iterdata()`): group map results by query index, write intermediate files to S3 (`reduce/res_{n}.json`).
5. **Reduce phase**: merge per-query results from multiple map tasks, deduplicate by vector ID, sort by distance, trim to k.

| File | Description |
|------|-------------|
| `orchestrator.py` | `Orchestrator` — splits queries across Lithops workers (map), collects partial results, merges (reduce) |

The map worker loads centroid `.ann` files and searches them. The reduce worker merges results from all centroids and pending files. `query_batch_size` controls map parallelism: `ceil(num_index / query_batch_size) + 1`.

See also: `vectordb/core/README.md` for the `QueryStrategy` ABC.
