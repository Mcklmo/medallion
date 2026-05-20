"""
Bootstrap Pub/Sub topics and subscriptions from a medallion config.yml.

Works against the Pub/Sub emulator (when PUBSUB_EMULATOR_HOST is set) and
against real Pub/Sub (when GOOGLE_CLOUD_PROJECT is set and credentials are
available). The code path is identical — the google-cloud-pubsub client
picks the emulator up automatically from the env var.

Usage:
    python bootstrap.py --config config.yml
    python bootstrap.py --config config.yml --prune     # deactivate stale resources
    python bootstrap.py --config config.yml --dry-run   # show plan, change nothing
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from google.pubsub import Topic
import yaml
from google.api_core import exceptions as gcp_exc
from medallion.fleet.pipeline_graph_model import PipelineGraph
from medallion.store.store import load_service_account_credentials

log = logging.getLogger("bootstrap")

# Subscriptions need an ack deadline; 60s is a reasonable default for
# transformer/store work. Long-running extractor work doesn't read from a
# queue, so this only applies to the readers.
DEFAULT_ACK_DEADLINE_SECONDS = 60

# How long to wait for the emulator to come up before giving up.
EMULATOR_WAIT_TIMEOUT = 30


@dataclass(frozen=True)
class PlannedResource:
    """A topic or subscription we intend to create."""

    kind: str  # "topic" or "subscription"
    name: str  # fully-qualified path
    topic: str | None = None  # for subscriptions, the topic path they bind to


def load_config(path: Path) -> PipelineGraph:
    with path.open() as f:
        cfg = yaml.safe_load(f)

    return PipelineGraph.model_validate(cfg)


def project_id() -> str:
    """
    Resolve the GCP project ID. The emulator accepts any non-empty string,
    so we default to a stable local one if nothing is configured.
    """
    GCP_PROJECT_ENV_VAR = "GOOGLE_CLOUD_PROJECT"
    id = os.environ.get(GCP_PROJECT_ENV_VAR)
    assert id is not None, f"{GCP_PROJECT_ENV_VAR} environment variable is required"

    return id


def wait_for_emulator() -> None:
    """
    The emulator container is usually reachable within a second or two of
    startup, but compose's depends_on doesn't wait for readiness. Poll the
    publisher API until a list call succeeds.
    """
    host = os.environ.get("PUBSUB_EMULATOR_HOST")
    if not host:
        return  # not using the emulator; nothing to wait for

    log.info("waiting for Pub/Sub emulator at %s", host)
    publisher = pubsub_v1.PublisherClient()
    project_path = f"projects/{project_id()}"
    deadline = time.monotonic() + EMULATOR_WAIT_TIMEOUT

    while True:
        try:
            # list_topics is the cheapest readiness probe available.
            list(publisher.list_topics(request={"project": project_path}))
            log.info("emulator is ready")
            return
        except Exception as e:  # noqa: BLE001 — emulator can throw a variety
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f"emulator not ready after {EMULATOR_WAIT_TIMEOUT}s"
                ) from e
            time.sleep(0.5)


def plan(
    cfg: PipelineGraph,
    project: str,
    credentials: service_account.Credentials,
) -> list[PlannedResource]:
    """
    Build the list of resources the config implies.
    """
    repo_name = cfg.repo.name
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    planned: list[PlannedResource] = []

    # Topics: one per declared queue, prefixed with the repo name per the
    # design doc ("queue name prefix of the repo name").
    queue_to_topic: dict[str, str] = {}
    for q in cfg.queues:
        topic_name = f"{repo_name}-{q.name}"
        topic_path = publisher.topic_path(project, topic_name)
        queue_to_topic[q.name] = topic_path

        planned.append(PlannedResource(kind="topic", name=topic_path))

        # add one storage subscription per queue, so stores can read from the queues without transformers
        sub_name = f"{repo_name}-{q.name}-storage-sub"
        sub_path = subscriber.subscription_path(project, sub_name)
        planned.append(
            PlannedResource(
                kind="subscription",
                name=sub_path,
                topic=topic_path,
            )
        )

    # Subscriptions: one per reading processor (transformers + stores).
    # Extractors only write, so they get no subscription.
    for t in cfg.transformers:
        queue = t.reads_from
        assert queue in queue_to_topic

        sub_name = f"{repo_name}-{t.name}-sub"
        sub_path = subscriber.subscription_path(project, sub_name)
        planned.append(
            PlannedResource(
                kind="subscription",
                name=sub_path,
                topic=queue_to_topic[queue],
            )
        )

    return planned


def apply(
    planned: list[PlannedResource],
    credentials: service_account.Credentials,
) -> None:
    """Create resources, ignoring AlreadyExists so the script is idempotent."""
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)

    for res in planned:
        if res.kind == "topic":
            try:
                publisher.create_topic(
                    Topic(
                        name=res.name,
                    )
                )
                log.info("created topic %s", res.name)
            except gcp_exc.AlreadyExists:
                log.info("topic exists: %s", res.name)
        elif res.kind == "subscription":
            try:
                subscriber.create_subscription(
                    request={
                        "name": res.name,
                        "topic": res.topic,
                        "ack_deadline_seconds": DEFAULT_ACK_DEADLINE_SECONDS,
                        "enable_message_ordering": True,
                    }
                )
                log.info("created subscription %s -> %s", res.name, res.topic)
            except gcp_exc.AlreadyExists:
                log.info("subscription exists: %s", res.name)
        else:
            raise AssertionError(f"unknown resource kind: {res.kind}")


def prune(
    planned: list[PlannedResource],
    repo_name: str,
    project: str,
    credentials: service_account.Credentials,
) -> None:
    """
    Delete topics and subscriptions belonging to this repo that aren't in
    the current config. Mirrors the design doc's deactivation step.

    Only touches resources prefixed with the repo name — never deletes
    another repo's resources sharing the same emulator/project.
    """
    prefix = f"{repo_name}-"
    wanted = {p.name for p in planned}

    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    project_path = f"projects/{project}"

    # Subscriptions first — can't delete a topic while subscriptions bind to it.
    for sub in subscriber.list_subscriptions(request={"project": project_path}):
        short = sub.name.rsplit("/", 1)[-1]
        if short.startswith(prefix) and sub.name not in wanted:
            subscriber.delete_subscription(request={"subscription": sub.name})
            log.info("pruned subscription %s", sub.name)

    for topic in publisher.list_topics(request={"project": project_path}):
        short = topic.name.rsplit("/", 1)[-1]
        if short.startswith(prefix) and topic.name not in wanted:
            publisher.delete_topic(request={"topic": topic.name})
            log.info("pruned topic %s", topic.name)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("config.yml"))
    parser.add_argument(
        "--prune",
        action="store_true",
        help="delete repo-scoped resources not in config",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print plan, make no changes"
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    project = project_id()

    wait_for_emulator()

    credentials = load_service_account_credentials()
    planned = plan(
        cfg,
        project,
        credentials,
    )

    if args.dry_run:
        log.info("dry run — planned resources:")
        for p in planned:
            if p.kind == "topic":
                print(f"  topic        {p.name}")
            else:
                print(f"  subscription {p.name} -> {p.topic}")

        return 0

    apply(planned, credentials)
    if args.prune:
        prune(
            planned,
            cfg.repo.name,
            project,
            credentials,
        )

    log.info("bootstrap complete: %d resources", len(planned))

    return 0


if __name__ == "__main__":
    sys.exit(main())
