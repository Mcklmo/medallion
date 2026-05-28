# Medallion

![PyPI](https://img.shields.io/pypi/v/medallion-pipeline.svg)
![License](https://img.shields.io/pypi/l/medallion-pipeline.svg)
![Python](https://img.shields.io/pypi/pyversions/medallion-pipeline.svg)

A scraper platform running hundreds of processing steps in Python mandates minimizing maintenance cost for updating and deploying processing steps, and guaranteeing availability of the output data for internal users in less than 0.5 seconds, especially in the case of a thundering herd causing an explosion in the amount of data to process at any point in time.

Medallion is a Python library that reduces the code developers need to change to update processing steps to the mere business logic. It deploys scrapers as microservices, isolating deployments and allowing for seamless re-routing of data traffic after deployment. It exposes the output of each processing step directly as a stream of data that internal users can opt–in for near-instant data availability. The output of each step is also written to disk for asynchronous inspection. Medallion manages thundering herds by design, because the processing–step microservices are inherently stateless, allowing you to scale horizontally as needed.

## Installation

```bash
pip install medallion-pipeline
```

The distribution is published as `medallion-pipeline`; the Python import name is `medallion`. Python 3.12+ required.

## Core concepts

- **Extractor** — produces the initial data. Subclass [`BaseExtractor[Out]`](medallion/base.py) (or [`BaseJSONExtractor[Out]`](medallion/base.py) for JSON output) and implement `extract()`.
- **Transformer** — consumes the previous step's output and produces a new one. Subclass [`BaseTransformer[In, Out]`](medallion/base.py) (or [`BaseJSONTransformer`](medallion/base.py) / [`BasePydanticTransformer`](medallion/base.py)) and implement `transform()`.
- **Queues** – Extractors send to a queue and Transformers read from a queue and send to another queue. Queues are typed and processors (Extractor or Transformer) reading from and/or writing to queues must match the queue's type. Extractors only have an output type, Transformers have input and output types. The library supports a mock-in-memory queue and GCP Pub/Sub. To roll your own, implement the [`Queue` interface](medallion/queue/base.py).
- **Store** – is attached to each queue automatically – you don't need to write any code to do that. A Store saves the output of each Extractor and Transformer either on local disc or in a GCP Storage Bucket. To roll your own, implement the [`BlobStore` interface](medallion/store/base.py).
- **Orchestration** – you tie everything together in a `config.yml` file. Here you define data types, queues, extractors and transformers, and which queues the processors read and write to.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer notes and the release process.
