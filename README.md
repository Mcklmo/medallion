# Medallion

![PyPI](https://img.shields.io/pypi/v/medallion-pipeline.svg)
![License](https://img.shields.io/pypi/l/medallion-pipeline.svg)
![Python](https://img.shields.io/pypi/pyversions/medallion-pipeline.svg)

A composable scraper pipeline built around the medallion architecture (bronze / silver / gold). You define an **extractor** and a chain of **transformers**; the pipeline persists every step's output and caches transformer results by content hash, so unchanged inputs skip recomputation on re-runs.

## Installation

```bash
pip install medallion-pipeline
```

The distribution is published as `medallion-pipeline`; the Python import name is `medallion`. Python 3.12+ required.

## Core concepts

- **Extractor** — produces the initial data. Subclass [`BaseExtractor[Out]`](medallion/base.py) (or [`BaseJSONExtractor[Out]`](medallion/base.py) for JSON output) and implement `extract()` and `read_bytes()`.
- **Transformer** — consumes the previous step's output and produces a new one. Subclass [`BaseTransformer[In, Out]`](medallion/base.py) (or [`BaseJSONTransformer`](medallion/base.py) / [`BasePydanticTransformer`](medallion/base.py)) and implement `transform()` (plus `read_bytes()` if you don't use one of the convenience bases).
- **PipeLine** — orchestrates the chain. Persists every run's artifacts to `store_output` under a timestamped folder, and caches transformer results in `store_cache` keyed by SHA256 of the input bytes — re-runs with unchanged inputs skip the transform and reuse the cached output.
- **Type safety** — each transformer's `input_type` must equal the prior step's `output_type`. The pipeline validates this before running (see [`medallion/resolve_classes.py`](medallion/resolve_classes.py#L81-L102)).

## Quick start (programmatic)

Three files. The full working version lives in [`example/`](example/).

```python
# extract.py
from io import BytesIO
import json
from medallion.base import BaseJSONExtractor

class Extractor(BaseJSONExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return [{"name": "Alice"}, {"name": "Bob"}]

    def read_bytes(self, data: BytesIO) -> list[dict]:
        data.seek(0)
        return json.loads(data.read().decode())
```

```python
# transform.py
from medallion.base import BaseJSONTransformer

class Transformer(BaseJSONTransformer[list[dict], list[dict]]):
    def transform(self, data: list[dict]) -> list[dict]:
        return [{"name": d["name"].upper()} for d in data]
```

```python
# run.py
from extract import Extractor
from transform import Transformer
from medallion.log import create_logger
from medallion.pipeline import PipeLine
from medallion.store.store import initialize_storage, must_get_env

logger = create_logger()
result = PipeLine(
    extractor=Extractor(),
    transformers=[Transformer()],
    logger=logger,
    store_output=initialize_storage(must_get_env("LOCAL_OUTPUT_DIR"), logger),
    store_cache=initialize_storage(must_get_env("LOCAL_CACHE_DIR"), logger),
).run()

print(result)
```

Run it:

```bash
export FILE_STORAGE_TYPE=local
export LOCAL_OUTPUT_DIR=/tmp/medallion/output
export LOCAL_CACHE_DIR=/tmp/medallion/cache
python run.py
# [{'name': 'ALICE'}, {'name': 'BOB'}]
```

Re-run the script — the logs will report `Cache hit for extractor from previous run` and `Cache hit for transformer Transformer`, and the transform is skipped.

## Quick start (CLI)

If you'd rather wire the pipeline by class name, expose your classes from an `__init__.py` and use the `medallion` CLI:

```python
# pipeline_package/__init__.py
from .extract import Extractor
from .transform import Transformer
```

```bash
export MEDALLION_ROOT=/path/to/pipeline_package
export FILE_STORAGE_TYPE=local
export LOCAL_OUTPUT_DIR=/tmp/medallion/output
export LOCAL_CACHE_DIR=/tmp/medallion/cache

medallion Extractor Transformer
```

The first positional argument is the extractor class; remaining arguments are transformers applied in order. If `MEDALLION_ROOT` is unset, the current working directory is used.

## Base classes

All defined in [`medallion/base.py`](medallion/base.py).

| Class | Implement | File extension | Notes |
| --- | --- | --- | --- |
| `BaseExtractor[Out]` | `extract`, `read_bytes`, `file_extension`, `write_output` | (user-defined) | Lowest-level extractor; bring your own serialization. |
| `BaseTransformer[In, Out]` | `transform`, `read_bytes`, `file_extension`, `write_output` | (user-defined) | Lowest-level transformer; bring your own serialization. |
| `BaseJSONExtractor[Out]` | `extract`, `read_bytes` | `json` | JSON serialization handled. |
| `BaseJSONTransformer[In, Out]` | `transform` | `json` | JSON serialization and `read_bytes` handled. |
| `BasePydanticTransformer[In, Out: BaseModel]` | `transform` | `json` | Serialization via `model_dump_json` / `model_validate_json`. |

## Storage backends

`initialize_storage` (in [`medallion/store/store.py`](medallion/store/store.py)) selects a backend from `FILE_STORAGE_TYPE`:

| `FILE_STORAGE_TYPE` | Backend | Required env vars |
| --- | --- | --- |
| `local` | `LocalStorage` (writes to disk) | `LOCAL_OUTPUT_DIR`, `LOCAL_CACHE_DIR` |
| `gcs` | `GCStorage` (Google Cloud Storage) | `GCS_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS` |

The pipeline takes two stores: `store_output` (every run's full artifacts) and `store_cache` (transformer-output cache). They can point at the same backend or different ones.

## Output layout

Every run writes one file per step under `store_output`, ordered by step index:

```text
{ExtractorName}_{TransformerName}_{…}/
  {YYYY-MM-DD-HH-mm-ssSSS}/
    0_{ExtractorName}.{ext}
    1_{TransformerName}.{ext}
    …
```

Transformer results are also written to `store_cache` keyed by the SHA256 of their input bytes:

```text
{sha256-of-input-bytes}/
  {TransformerName}.{ext}
```

On the next run, if a transformer's input hashes to the same value, its cached output is loaded instead of recomputing.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer notes and the release process.
