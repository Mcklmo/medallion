# Medallion

![PyPI](https://img.shields.io/pypi/v/medallion-pipeline.svg)
![License](https://img.shields.io/pypi/l/medallion-pipeline.svg)
![Python](https://img.shields.io/pypi/pyversions/medallion-pipeline.svg)

Medallion is a Python library for building and running scraper pipelines made of dozens or hundreds of processing steps. It targets three things that are usually hard to get at the same time: keeping the cost of changing a step low, making each step's output available to internal users in fractions of a second, and staying stable when a sudden spike in input (a "thundering herd") explodes the amount of data to process at any point in time.

You write only the business logic. Medallion reduces the code you change to update a processing step to a single `extract()` or `transform()` method — queues, storage, wiring, schemas, and deployment artifacts are all derived from one `config.yml`. It deploys scrapers as isolated microservices, exposes each step's output directly as a stream internal users can subscribe to for near-instant availability, and archives every step's output automatically so you can replay the exact input for a step you've changed — without a replay-capable broker and without re-running the whole pipeline. Because the processing-step microservices are stateless by construction, thundering herds are absorbed by scaling horizontally rather than by tuning.

## When to use Medallion (and when not)

Medallion is meant to be the **first layer** of a data system: it scrapes, processes, and serves raw and intermediate data. Downstream, you can feed its output into a query engine or warehouse like ClickHouse, BigQuery, or DuckDB — though some applications are fine with no second layer at all.

It fits best when:

- your pipeline is **scraper-shaped**: stateless steps that extract and transform, rather than long-running stateful jobs;
- you can write **every processing step in Python**;
- you deploy to **GCP**: Pub/Sub and Storage Buckets are supported out of the box. Other queues and blob stores are not built in yet, but the I/O sits behind the [`Queue`](medallion/queue/base.py) and [`BlobStore`](medallion/store/base.py) interfaces, so adding one is a handful of methods.

> **On the name:** Medallion is a nod to the *medallion architecture* (bronze/silver/gold maturity layers). It is not a lakehouse implementation of that pattern — but the layered, multi-step shape is the same idea, except you choose your own layer names and use as many as you need.

## Installation

```bash
pip install medallion-pipeline
```

The distribution is published as `medallion-pipeline`; the Python import name and CLI tool name is `medallion`. Python 3.12+ required.

## Usage

### Start a new project

Create boilerplate for your new project and set up an example file structure.

```bash
medallion start MY_NEW_PROJECT_NAME
```

Install dependencies and create a virtual environment.

```bash
poetry install
```

### Your business logic implementation

The `start` command creates an example extractor and transformer for you to get started. You can modify these files or create new ones as needed. If you add new extractors or transformers, make sure to make them available to the `config.yml` file by importing them in the `src/__init__.py` file.

More examples can be found in the [examples folder](medallion/example/).

#### A minimal directory structure

```bash
├── .data/  # Local storage for pipeline outputs
├── src/
│   ├── __init__.py  # Import your extractors, transformers, and data models here
│   ├── extractor.py
│   ├── transformer.py
│   └── model.py
├── config.yml  # Define your pipelines here, using the classes you imported in src/__init__.py
```

#### An example `config.yml` file

```yaml
apiVersion: medallion/v1
repo:
  name: example

defaults: # these apply to all extractors and transformers, unless overwritten
  runtime:
    cpu: 1
    memory: 512Mi
    timeout: 300s
    min_instances: 0
    max_instances: 10
    concurrency: 10

schemas: # these are data models you define in Python and import in src/__init__.py
  - name: FileOutput
  - name: SourceDataModel

queues: # extractors write to queues, transformers read from and write to queues. The data written to each queue is automatically stored in local or remote storage, and exposed as a stream for internal users to consume.
  - name: raw-csv-files
    schema: FileOutput
  - name: processed-data
    schema: SourceDataModel

extractors:
  - name: source-data
    class: SourceDataExtractor # imported in src/__init__.py
    writes_to: raw-csv-files
    schedules:
      - name: peak-hours
        cron: "0/10 12 * * *"   
        timezone: Europe/Copenhagen
    runtime:
      timeout: 1800s
      memory: 1Gi
      max_instances: 1

transformers:
  - name: csv-to-model
    class: SourceDataTransformer # imported in src/__init__.py
    reads_from: raw-csv-files
    writes_to: processed-data
    runtime:
      concurrency: 50
      max_instances: 20
      min_instances: 1
```

### Create debugging configurations for VS Code

Medallion creates debugging configurations for your scraper pipelines, which you can run in the VS Code debugger. The `start` command already creates working configurations for the example extractor and transformer, so you can start debugging right away. Re-run the command after making changes to your `config.yml` file to update the debugging configurations with your new pipelines.

The command automatically validates your `config.yml` file before generating the debugging configurations.

Inside your project, run:

```bash
medallion vscode
```

Or alternatively, run the command `Configure VS Code Debugger` from the command palette.

Example result (your's may vary):

![example result vscode](/doc/usage/example_result_vscode.png)

The last few configurations starting with `Run Pipeline:` are your scraper pipelines, defined in `config.yml`. Run them to launch a debugger for your scraper.

After making changes to your `config.yml`, run the command `Configure VS Code Debugger` to validate it and re-generate the `./launch/config.json` file with updated pipelines.

### Configure Docker workspace

Medallion can generate a `docker-compose.yml` file from your `config.yml` to run your pipelines in an isolated environment. Each service runs in its own container, and you can attach a debugger to any of them.

The `start`command already generates an innitial `docker-compose.yml` file for you to get started. Re-run the below command after making changes to your `config.yml` file to update the `docker-compose.yml` file with your new pipelines.

Run the debug configuration `Generate docker compose from config.yml` or execute this command:

```bash
poetry run python -m medallion.configure_entrypoint.generate_docker_compose
```

This validates your `config.yml` and generates a `docker-compose.yml` and a `docker-compose.debug.yml`

The docker-compose file will:

1. Run an isolated GCP Pub/Sub emulator host as microservice
2. Run a microservice for each transformer from your `config.yml` file
3. Run a Store microservice for each queue defined in your `config.yml` file. The storage type is mounted to your local folder `.medallion-root`, which is meant to re-use the data folder you use for local runs.
4. Run a microservice for each extractor, that listens on port 8001, 8002, ..., 8000+n for n extractors.

To use it, run:

```bash
EXTRACTOR_API_KEY=YOUR_API_KEY docker compose up [--build]
```

To trigger an extractor, run:

```bash
curl --location --request POST 'http://0.0.0.0:EXTRACTOR_PORT' \
--header 'X-Extractor-Api-Key: YOUR_API_KEY'
```

#### Debugging inside of Docker container

To run a debugger in a docker container, have a look at the `docker-compose.debug.yml` file generated together with `docker-compose.yml`. It picks the first extractor from the compose by default. Modify the service name to debug any other service from your `docker-compose.yml`.

Run:

```bash
docker compose -f docker-compose.yml -f docker-compose.debug.yml up [--build]
```

Once all containers are ready and their logs indicate that they're listening, run the debugging configuration `Attach to docker service`.

### Configure GCP Deployment

`medallion` generates a Google Cloud Run fleet of services from your `config.yml`,
using Pub/Sub as the broker and a storage bucket for each queue's Store — the
same topology defined for local runs.

... Documentation coming soon ...

### Run an Extractor as HTTP Server

If you need to debug the extractor HTTP server alone, which usually only runs inside docker or in production, you may want to use this entrypoint instead of running the entire fleet in docker.

```bash
EXTRACTOR_CLASS=MY_EXTRACTOR poetry run uvicorn medallion.run.extractor:app --host 0.0.0.0 --port 8080
```

Alternatively, run the debug configuration `Run extractor as HTTP server` and set the environment variable `EXTRACTOR_CLASS` to the extractor you want to run.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for developer notes and the release process.
