# config — Default Index Configuration

Default index parameters used by `initialize-database`.

| File | Description |
|------|-------------|
| `indexconfig.json` | Default JSON config with `features`, `num_index`, `k`, `n_probe`, `kmeans_version`, memory settings, etc. |

See `vectordb/config.py` for the `SvlessVectorDBParams` dataclass that reads these values.
