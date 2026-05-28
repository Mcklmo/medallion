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

The distribution is published as `medallion-pipeline`; the Python import name and CLI tool name is `medallion`. Python 3.12+ required.

## Usage

Refer to [usage](doc/usage/usage.md).

## Core concepts

- **Extractor** — produces the initial data and sends to a `Queue`. You implement a class that inherits from [`BaseExtractor[Out]`](medallion/base.py) and implement `extract(self) -> Iterator[Out]`.
- **Transformer** — consumes the previous step's output from a `Queue` and sends transformed output to a different queue. Subclass [`BaseTransformer[In, Out]`](medallion/base.py) and implement `transform(self, data: Iterator[In]) -> Iterator[Out]`. If you need streaming, you need to inherit from `BaseStreamingTransformer[In, Out]` and implement `transform_one(self, data: In) -> Out` instead.
- **Queues** – Queues are typed and processors (Extractor or Transformer) must match the queue's type in their corresponding `In` and/or `Out` types. The library supports a mock-in-memory queue and GCP Pub/Sub. To roll your own, implement the [`Queue` interface](medallion/queue/base.py).
- **Store** – is attached to each queue automatically – you don't need to write any code to do that. A Store reads from a queue and stores the data. The library has built-in support for storage on a local disc or GCP Storage Bucket. To roll your own, implement the [`BlobStore` interface](medallion/store/base.py).
- **Orchestration** – you tie everything together in a `config.yml` file. Here you define data types, queues, extractors and transformers, and which queues the processors read and write to.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer notes and the release process.
