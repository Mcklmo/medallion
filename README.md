# Medallion

![PyPI](https://img.shields.io/pypi/v/medallion-pipeline.svg)
![License](https://img.shields.io/pypi/l/medallion-pipeline.svg)
![Python](https://img.shields.io/pypi/pyversions/medallion-pipeline.svg)

A scraper platform running hundreds of processing steps in Python mandates minimizing maintenance cost for updating and deploying processing steps, and guaranteeing availability of the output data for internal users in 0.5 seconds or less, especially in the case of a thundering herd causing an explosion in the amount of data to process at any point in time.

Medallion is a Python library that reduces the code developers need to change to update processing steps to the mere business logic. It deploys scrapers as microservices, isolating deployments and allowing for seamless re-routing of data traffic after deployment. It exposes the output of each processing step directly as a stream of data that internal users can opt–in for near-instant data availability. The output of each step is also written to disk for asynchronous inspection. Medallion manages thundering herds by design, because the processing–step microservices are inherently stateless, allowing you to scale horizontally as needed.

## Why Medallion

Building a scraper platform that stays cheap to maintain *and* serves data in
0.5 seconds or less usually means gluing together a message broker, a worker
framework, an archival sink, a schema layer, a local emulator, and a pile of
deployment YAML. Medallion collapses that stack into a single config-driven
library so the only thing you actually write is your business logic.

- **Only the business logic changes.** Updating a processing step means editing one `extract()` or `transform()` method. Queues, storage, wiring, schemas, and deployment artifacts are derived from `config.yml` — there is no plumbing to touch when you ship a change.

- **Sub-second availability of raw source data.** Every step exposes its output as a live stream that internal users can subscribe to directly. Internal users don't have to wait for the entire pipeline to finish to get access to the data, if they prefer instant availability instead of more complex queries on the final output. This also means that if a downstream step is slow or broken, the raw source data is still available to internal users without having to wait for the entire pipeline to be fixed.

- **Thundering herds handled by design.** Processing-step microservices are stateless by construction, so a sudden explosion in input volume is absorbed by scaling steps horizontally rather than by tuning or babysitting the system.

- **Every intermediate stream is archived automatically.** A Store is attached to each queue with no code on your part, writing output to disk (or a bucket) for asynchronous inspection and replay — without requiring a replay-capable broker. This doubles as a precise replay tool: a developer who changes one step can re-process exactly the same archived input without re-running the entire pipeline — avoiding the risk of costly re-processing or that upstream changes or non-determinism produce different input.

- **Typed pipelines, end to end.** Queues are typed and every processor's `In`/ `Out` must match the queues it reads from and writes to, so topology mistakes are caught as early as possible.

- **Isolated, independently deployable steps.** Each extractor and transformer runs as its own microservice, so deployments are isolated and data traffic can be re-routed seamlessly after a deploy.

- **One config, local and prod parity.** From a single `config.yml`, Medallion generates a runnable Docker Compose cluster, debug configurations for VS Code, and a Google Cloud Run fleet of services with Pub/Sub as broker and storage bucket — the same topology you run locally is the one you deploy. If you prefer a different cloud provider than GCP, you can easily swap it, you only need to implement a handful of functions for the Queue and BlobStore interfaces for your provider of choice.

In total, Medallion replaces what is typically **8–10 separate components plus
their glue code**.

## Installation

```bash
pip install medallion-pipeline
```

The distribution is published as `medallion-pipeline`; the Python import name and CLI tool name is `medallion`. Python 3.12+ required.

## Core concepts

- **Extractor** — produces the initial data and sends to a `Queue`. You implement a class that inherits from [`BaseExtractor[Out]`](medallion/base.py) and implement `extract(self) -> Iterable[Out]`.
- **Transformer** — consumes the previous step's output from a `Queue` and sends transformed output to a different queue. Subclass [`BaseTransformer[In, Out]`](medallion/base.py) and implement `transform(self, data: Iterable[In]) -> Iterable[Out]`. If you need streaming, you need to inherit from `BaseStreamingTransformer[In, Out]` and implement `transform_one(self, data: In) -> Out` instead.
- **Queues** – Queues are typed and processors (Extractor or Transformer) must match the queue's type in their corresponding `In` and/or `Out` types. The library supports a mock-in-memory queue and GCP Pub/Sub. To roll your own, implement the [`Queue` interface](medallion/queue/base.py).
- **Store** – is attached to each queue automatically – you don't need to write any code to do that. A Store reads from a queue and stores the data. The library has built-in support for storage on a local disc or GCP Storage Bucket. To roll your own, implement the [`BlobStore` interface](medallion/store/base.py).
- **Orchestration** – you tie everything together in a `config.yml` file. Here you define data types, queues, extractors and transformers, and which queues the processors read and write to.

## Benchmark results

> All benchmarks were run on an M4 Max with 64 GB unified memory on macOS 26.5 using Python 3.13.8.
> Workloads, configs, and scripts to reproduce these results live in
> [`benchmarks/`](benchmarks/).

### What we measured

| Dimension | What it tells you | How important for the user (0 not important to 10 very important) | Story points |
| --- | --- | --- | --- |
| **Lines of code to a working pipeline** | Maintenance and onboarding cost for scraper development | 10 | ? |
| **End-to-end latency (extract → consumer)** | Availability SLA under normal load | 10 | ? |
| **Latency under burst (thundering herd)** | Behavior when input volume explodes | 10 | ? |
| **Time to redeploy a single step** | Cost of shipping a change | 5 | ? |
| **Throughput per step (records/sec)** | Capacity per stateless replica | 6 | ? |
| **Degradation under a slow step** | Pipeline isolation guarantees | 10 | ? |

### Lines of code to an equivalent pipeline

| Stack | LOC* | Components to operate |
| --- | --- | --- |
| **Medallion** | ??? | 1 |
| Faust + custom archival + compose | ??? | ??? |
| Bytewax + custom archival + compose | ??? | ??? |
| Celery + FastAPI + broker + S3 sink (hand-rolled) | ??? | ??? |

\*LOC includes source code and config files.

### End-to-end latency

TODO: add `timestamp sent` to the output of each step. This allows any two steps in a pipeline to measure the latency between them, including the first and the last step.

Consider one message equaling 10 lines of csv data, going through a pipeline with 2 steps (Extractor → Transformer). We measure the latency between the output of the Extractor and the output of the Transformer. The extractor in our case is a mock that produces messages at a configurable rate, so we can measure latency under different loads.

| Scenario | p50 | p95 | p99 |
| --- | --- | --- | --- |
| Steady state (10 messages/sec) | ??? ms | ??? ms | ??? ms |
| Thundering herd (1000 messages/sec) | ??? ms | ??? ms | ??? ms |

> Target: structured output available to internal users in **0.5 s or less**.
> <!-- TODO: one sentence stating whether/under what conditions the target holds -->

### Throughput and scaling

| Replicas per step | Throughput (records/sec) | Latency p99 |
| --- | --- | --- |
| 1 | ??? | ??? ms |
| 4 | ??? | ??? ms |
| 16 | ??? | ??? ms |

<!-- TODO: one sentences on observed scaling characteristics, e.g. near-linear up to N replicas. -->

### Deployment cost

| Action | Medallion | Other |
| --- | --- | --- |
| Redeploy a single step | ??? s | ??? s |
| Add a new transformer | ??? LOC / ??? s | ??? |

### Reproducing these results

```bash
cd benchmarks
poetry install --with benchmarks
poetry run python run_benchmarks.py
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer notes and the release process.
