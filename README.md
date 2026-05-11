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
