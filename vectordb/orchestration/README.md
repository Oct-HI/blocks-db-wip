# orchestration — Distributed Map/Reduce Search

| File | Description |
|------|-------------|
| `orchestrator.py` | `Orchestrator` — splits queries across Lithops workers (map), collects partial results, merges (reduce) |

The map worker loads centroid `.ann` files and searches them. The reduce worker merges results from all centroids and pending files.
