#!/usr/bin/env python3
"""
Generate a docker-compose.yml for local development from a medallion config.yml.

Maps the GCP deployment primitives onto local equivalents so a full processor
graph can be run on a laptop:

    Pub/Sub topics      -> Pub/Sub emulator topics (same client, same code)
    Cloud Run services  -> one compose service per processor
    Cloud Scheduler     -> commented cron hints (run manually in dev)
    BigQuery store      -> medallion/run/store.py against a local sink

Stores are NOT read from config.yml. One store is generated per queue
automatically (see `stores_for_queues`). Any `stores:` block in the config is
ignored, with a warning.

Usage:
    python generate_compose.py config.yml [-o docker-compose.yml]
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml

from medallion.configure_entrypoint.configure_runtime import (
    DEFAULT_CONFIG_NAME,
    load_config,
)
from medallion.configure_entrypoint.pipeline_graph_model import (
    EffectiveRuntime,
    Extractor,
    PipelineGraph,
    Store,
    Transformer,
)
from medallion.configure_entrypoint.resource_names import (
    DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
    dlq_topic_name,
    storage_subscription_name,
    topic_name,
    transformer_subscription_name,
)
from medallion.model.extractor import (
    FORCE_RUN_EXTRACTOR_ENV_VAR,
)
from medallion.resolve_classes import (
    EXTRACTOR_CLASS_ENV_VAR,
    TRANSFORMER_CLASS_ENV_VAR,
    get_medallion_root,
)
from medallion.run.extractor import (
    API_KEY_ENV,
    GOOGLE_CLOUD_PROJECT_ENV_VAR,
    PUB_SUB_QUEUE_TYPE,
    QUEUE_TYPE_ENV_VAR,
)
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR
from medallion.store.base import (
    MEDALLION_TOPIC_ENV,
    must_get_env,
    FILE_STORAGE_TYPE_ENV_VAR,
)
from medallion.store.initialize_storage import (
    FILE_STORAGE_LOCAL,
    LOCAL_OUTPUT_DIR_ENV_VAR,
)

# --------------------------------------------------------------------------- #
# Tunables — change these to match your repo's entrypoints / conventions.
# --------------------------------------------------------------------------- #

# Entrypoint module per processor type. `python -m <module> <name>` is run.
RUN_MODULE = {
    "extractor": "medallion.run.extractor",
    "transformer": "medallion.run.transformer",
    "store": "medallion.run.store",
}

# Image build context (the repo root, where the Dockerfile lives).
BUILD_CONTEXT = "."

# Path the processor classes are imported from (design.md: ./src/ by default).
MEDALLION_ROOT = "/app/src"

# Pub/Sub emulator host:port used inside the compose network.
EMULATOR_HOST = "pubsub:8085"

# Fake GCP project id — the emulator does not care what this is, but topic /
# subscription resource paths need *a* project, and it must match across
# the bootstrap container and every processor.
PROJECT_ID = "local-dev"

# HTTP port extractors listen on inside the container (Cloud Run convention).
EXTRACTOR_INTERNAL_PORT = 8080

# First host port to map extractor services to; each extractor gets the next one.
EXTRACTOR_HOST_PORT_START = 8001

# If True, generate one store per *every* queue. If False, only terminal queues
# (queues nothing reads from) get a store. "one store for each queue" reads as
# every-queue, which is also the more useful default for local inspection.
STORE_EVERY_QUEUE = True

# Local sink the store writes to in dev. Matches the volume mount target
# in `build_store_service` (./.medallion-data:/app/data).
STORE_OUTPUT_DIR = "/app/data"

# --------------------------------------------------------------------------- #
# Store synthesis — the auto-generated stores, one per queue.
# --------------------------------------------------------------------------- #


def stores_for_queues(graph: PipelineGraph) -> list[Store]:
    queues = [q.name for q in graph.queues]
    if not STORE_EVERY_QUEUE:
        read_queues = {t.reads_from for t in graph.transformers}
        queues = [q for q in queues if q not in read_queues]
    return [Store(name=f"store-{q}", class_="BaseStore", reads_from=q) for q in queues]


# --------------------------------------------------------------------------- #
# Service builders
# --------------------------------------------------------------------------- #


def base_env(extra: dict[str, Any] | None = None) -> dict[str, Any]:
    env = {
        "PUBSUB_EMULATOR_HOST": EMULATOR_HOST,
        GOOGLE_CLOUD_PROJECT_ENV_VAR: PROJECT_ID,
        "MEDALLION_ROOT": MEDALLION_ROOT,
        FILE_STORAGE_TYPE_ENV_VAR: FILE_STORAGE_LOCAL,
    }
    if extra:
        env.update(extra)
    return env


def runtime_to_env(rt: EffectiveRuntime) -> dict[str, Any]:
    """Expose the merged runtime so the processor honours concurrency / flow
    control locally the same way Cloud Run would set it from the deploy."""
    return {
        "MEDALLION_CONCURRENCY": rt.concurrency,
        "MEDALLION_MAX_INSTANCES": rt.max_instances,
        "MEDALLION_MIN_INSTANCES": rt.min_instances,
        "MEDALLION_TIMEOUT": rt.timeout,
    }


def build_extractor_service(
    graph: PipelineGraph,
    repo: str,
    ex: Extractor,
    host_port: int,
) -> dict[str, Any]:
    rt = graph.effective_runtime(ex)
    env = base_env(runtime_to_env(rt))
    env[FORCE_RUN_EXTRACTOR_ENV_VAR] = "${FORCE_RUN_EXTRACTOR}"
    env[EXTRACTOR_CLASS_ENV_VAR] = ex.class_
    env[MEDALLION_TOPIC_ENV] = topic_name(repo, ex.writes_to)
    env[QUEUE_TYPE_ENV_VAR] = PUB_SUB_QUEUE_TYPE
    env[LOCAL_OUTPUT_DIR_ENV_VAR] = STORE_OUTPUT_DIR
    env[API_KEY_ENV] = "${EXTRACTOR_API_KEY}"
    svc: dict[str, Any] = {
        "build": BUILD_CONTEXT,
        "command": f"python -m {RUN_MODULE['extractor']}",
        "environment": env,
        "ports": [f"{host_port}:{EXTRACTOR_INTERNAL_PORT}"],
        "depends_on": {"bootstrap": {"condition": "service_completed_successfully"}},
        "volumes": [
            "./.medallion-data:/app/data"
        ],  # the extractor needs the local sink to check for previous runs "cache"
    }

    # Schedules can't run as Cloud Scheduler locally; surface them as a hint.
    if ex.schedules:
        crons = "; ".join(f"{s.name}={s.cron} ({s.timezone})" for s in ex.schedules)
        svc["labels"] = {"medallion.schedules": crons}

    return svc


def build_transformer_service(
    graph: PipelineGraph,
    repo: str,
    tr: Transformer,
) -> dict[str, Any]:
    rt = graph.effective_runtime(tr)
    env = base_env(runtime_to_env(rt))
    env[TRANSFORMER_CLASS_ENV_VAR] = tr.class_
    env["MEDALLION_SUBSCRIPTION"] = transformer_subscription_name(repo, tr.name)
    env[MEDALLION_TOPIC_ENV] = topic_name(repo, tr.writes_to)
    env["MEDALLION_DLQ_TOPIC"] = dlq_topic_name(repo, tr.reads_from)
    env[LISTENER_MAX_RETRIES_ENV_VAR] = must_get_env(LISTENER_MAX_RETRIES_ENV_VAR)
    return {
        "build": BUILD_CONTEXT,
        "command": f"python -m {RUN_MODULE['transformer']}",
        "environment": env,
        "depends_on": {"bootstrap": {"condition": "service_completed_successfully"}},
    }


def build_store_service(graph: PipelineGraph, repo: str, st: Store) -> dict[str, Any]:
    rt = graph.effective_runtime(st)
    env = base_env(runtime_to_env(rt))
    env["MEDALLION_SUBSCRIPTION"] = storage_subscription_name(repo, st.reads_from)
    env["MEDALLION_DLQ_TOPIC"] = dlq_topic_name(repo, st.reads_from)
    env[LISTENER_MAX_RETRIES_ENV_VAR] = must_get_env(LISTENER_MAX_RETRIES_ENV_VAR)
    env[LOCAL_OUTPUT_DIR_ENV_VAR] = STORE_OUTPUT_DIR

    return {
        "build": BUILD_CONTEXT,
        "command": f"python -m {RUN_MODULE['store']}",
        "environment": env,
        "depends_on": {"bootstrap": {"condition": "service_completed_successfully"}},
        "volumes": ["./.medallion-data:/app/data"],  # local sink persistence
    }


# --------------------------------------------------------------------------- #
# Bootstrap container — creates topics & subscriptions in the emulator.
# --------------------------------------------------------------------------- #


def build_bootstrap(
    repo: str,
    topics: list[str],
    subscriptions: list[tuple[str, str, str | None]],
) -> dict[str, Any]:
    """A throwaway container that waits for the emulator, then creates every
    topic and subscription via the REST API (curl). Runs to completion; the
    processors depend on it finishing."""
    # `$$` escapes a literal `$` for docker-compose interpolation, so the
    # generated YAML contains `${VAR}` and the in-container shell expands it.
    base = (
        f"$${{PUBSUB_EMULATOR_HOST}}/v1/projects/$${{{GOOGLE_CLOUD_PROJECT_ENV_VAR}}}"
    )
    lines = [
        "set -e",
        'echo "waiting for emulator..."',
        f"until curl -s {base}/topics -o /dev/null; do sleep 1; done",
    ]
    for t in topics:
        lines.append(
            f'curl -s -X PUT {base}/topics/{t} -o /dev/null && echo "topic {t}"'
        )
    for sub, topic, dlq in subscriptions:
        topic_path = f"projects/$${{{GOOGLE_CLOUD_PROJECT_ENV_VAR}}}/topics/{topic}"
        body = '{\\"topic\\": \\"' + topic_path + '\\"'
        if dlq is not None:
            dlq_path = f"projects/$${{{GOOGLE_CLOUD_PROJECT_ENV_VAR}}}/topics/{dlq}"
            body += (
                ', \\"deadLetterPolicy\\": {'
                '\\"deadLetterTopic\\": \\"' + dlq_path + '\\", '
                '\\"maxDeliveryAttempts\\": '
                + str(DEAD_LETTER_MAX_DELIVERY_ATTEMPTS)
                + "}"
            )
        body += "}"
        lines.append(
            f"curl -s -X PUT {base}/subscriptions/{sub} "
            f'-H "Content-Type: application/json" '
            f'-d "{body}" -o /dev/null && echo "sub {sub}"'
        )
    lines.append('echo "bootstrap done"')
    script = "\n".join(lines)
    return {
        "image": "curlimages/curl:8.7.1",
        "depends_on": ["pubsub"],
        "environment": {
            "PUBSUB_EMULATOR_HOST": f"http://{EMULATOR_HOST}",
            GOOGLE_CLOUD_PROJECT_ENV_VAR: PROJECT_ID,
        },
        "entrypoint": ["sh", "-c", script],
    }


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #


def generate(graph: PipelineGraph) -> dict[str, Any]:
    repo = graph.repo.name
    queues = [q.name for q in graph.queues]
    stores = stores_for_queues(graph)

    services: dict[str, Any] = {}

    # Pub/Sub emulator.
    services["pubsub"] = {
        "image": "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
        "command": (
            "gcloud beta emulators pubsub start "
            f"--host-port=0.0.0.0:8085 --project={PROJECT_ID}"
        ),
        "ports": ["8085:8085"],
    }

    # Collect topics + subscriptions for the bootstrap container. Each queue
    # also gets a DLQ topic; reading subscriptions get a dead_letter_policy
    # pointing at it so Pub/Sub populates delivery_attempt on incoming msgs.
    topics = [topic_name(repo, q) for q in queues]
    dlq_topics = [dlq_topic_name(repo, q) for q in queues]
    subscriptions: list[tuple[str, str, str | None]] = []
    for tr in graph.transformers:
        subscriptions.append(
            (
                transformer_subscription_name(repo, tr.name),
                topic_name(repo, tr.reads_from),
                dlq_topic_name(repo, tr.reads_from),
            )
        )
    for st in stores:
        subscriptions.append(
            (
                storage_subscription_name(repo, st.reads_from),
                topic_name(repo, st.reads_from),
                dlq_topic_name(repo, st.reads_from),
            )
        )

    services["bootstrap"] = build_bootstrap(repo, topics + dlq_topics, subscriptions)

    # Processor services.
    host_port = EXTRACTOR_HOST_PORT_START
    for ex in graph.extractors:
        services[ex.name] = build_extractor_service(graph, repo, ex, host_port)
        host_port += 1
    for tr in graph.transformers:
        services[tr.name] = build_transformer_service(graph, repo, tr)

    for st in stores:
        services[st.name] = build_store_service(graph, repo, st)

    return {"services": services}


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    config = DEFAULT_CONFIG_NAME
    ap.add_argument("-o", "--output", type=Path, default=Path("docker-compose.yml"))
    args = ap.parse_args()

    graph = load_config(config)
    compose = generate(graph)

    header = (
        "# AUTO-GENERATED from {src} by generate_compose.py — do not edit by hand.\n"
        "# Stores are synthesised (one per queue); any stores: block in the\n"
        "# config is ignored. Edit the config or the generator, then regenerate.\n"
    ).format(src=config.name)

    with args.output.open("w") as fh:
        fh.write(header)
        yaml.safe_dump(compose, fh, sort_keys=False, default_flow_style=False)

    n = len(compose["services"]) - 2  # minus pubsub + bootstrap
    print(f"  > wrote {args.output} ({n} processor services + emulator + bootstrap)")


if __name__ == "__main__":
    main()
