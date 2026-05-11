# Medallion architecture scraper pipeline

![PyPI](https://img.shields.io/pypi/v/medallion-pipeline.svg)
![License](https://img.shields.io/pypi/l/medallion-pipeline.svg)
![Python](https://img.shields.io/pypi/pyversions/medallion-pipeline.svg)

Plug and play library with batteries included for caching parser output at each step as blobs.

## Installation

```bash
pip install medallion-pipeline
```

The distribution is published as `medallion-pipeline`, but the Python import name is `medallion`:

```python
from medallion import Extractor, TransformerSingle, TransformerMultiple
```

Users define scraper scripts in Python, implementing an interface as specified below.

The tool can be executed like:

```bash
medallion PDFExtractor NaivePDFTransformer
```

Given an `__init__.py` file either at the root of the current directory or in any folder that the environment variable `MEDALLION_ROOT` points to.

Example `__init__.py`:

```python
from .extractors import PDFExtractor
from .transformers import NaivePDFTransformer
```

## The interfaces a user must implement

The class first in the pipeline must inherit from `Extractor`. All subsequent classes must inherit from either `TransformerSingle` or `TransformerMultiple`.

Extractors have no input type, they obtain data from some source, like a webserver or a file drive. Transformers have an input type that matches the output type of the previous processing step (which can be either Transformer or Extractor).

All of the types are user defined. `ConfigType` can be used to inject runtime configuration, s.a. a set of urls to visit, or any other custom user-based field.

```python
class ProcessingStep:
    def __init__(
        self,
        config: ConfigType|None=None,
    ):
        self._config=config

class TransformerSingle(ABC, ProcessingStep):
    def transform_single(
        item: InputType,
    ) -> OutputType:
        pass

class TransformerMultiple(ABC, ProcessingStep):
    def transform_multiple( 
        items: list[InputType],
    ) -> list[OutputType]:
        pass

class Extractor(ABC, ProcessingStep):
    def extract(self, 
    ) -> list[OutputType]:
        pass
```

## Developer notes

User-facing example pipelines live in `/example/__init__.py`. To execute them via the `Run User Pipeline (example)` launch configuration (or directly from the CLI), the `MEDALLION_ROOT` environment variable must be set to that folder so the resolver can locate the user package.

## Releasing

Releases are automated: pushing a tag matching `v*` to `main` triggers [`.github/workflows/release.yml`](.github/workflows/release.yml), which builds the package with Poetry and publishes it to PyPI via OIDC trusted publishing (no API token needed).

### Cutting a release

1. Bump `version` in [`pyproject.toml`](pyproject.toml) (semver: patch for bug fixes, minor for new features, major for breaking changes).
2. Commit and push to `main`.
3. Tag and push the tag — the tag's version *must* match `pyproject.toml`, or the workflow fails its sanity check:

   ```bash
   git tag v0.1.3
   git push origin v0.1.3
   ```

4. PyPI rejects re-uploads of an existing version. If a release ships broken, bump again — don't try to overwrite.

### Local build (optional, but recommended for some changes)

```bash
poetry build
ls dist/
unzip -l dist/medallion_pipeline-*-py3-none-any.whl   # inspect contents
```

Worth doing before tagging when:

- You edited `pyproject.toml` (entry points, dependencies, classifiers, license).
- You added non-Python files (`py.typed`, data files, templates) — Poetry can silently omit these.
- You're unsure the package still imports cleanly.

For pure code changes, skip the local build and just push + tag.

### Pre-release / dry run

Push an `rc` tag first to publish to PyPI as a pre-release (won't be installed by default by `pip install medallion-pipeline`):

```bash
git tag v0.1.3rc1
git push origin v0.1.3rc1
```

Verify on <https://pypi.org/project/medallion-pipeline/>, then cut the real tag.

### Initial PyPI trusted-publisher setup (one-time, already done)

For reference, if the trusted publisher ever needs to be reconfigured: <https://pypi.org/manage/account/publishing/> → pending publisher with owner `Mcklmo`, repo `medallion`, workflow `release.yml`, environment `pypi`.
