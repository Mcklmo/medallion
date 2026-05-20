"""
Medallion deployment script — three-phase deploy.

Commands:
  prepare    Validate config, then build all infrastructure in a DORMANT state:
             - Topics created (always live; passive resource)
             - Cloud Run services deployed but scaled to 0/0 (cannot serve)
             - Pub/Sub subscriptions created in DETACHED state (no delivery)
             - Cloud Scheduler jobs created PAUSED (no triggers)
             Old infrastructure continues serving traffic untouched.

  activate   Re-read config.yml and flip prepared resources live:
             - Scale services to configured min/max
             - Attach subscriptions to topics
             - Resume scheduler jobs
             Then deactivate orphans (resources from this repo not in new config).

  rollback   Re-read config.yml and tear down prepared-but-not-activated resources:
             - Delete services that are still at 0/0
             - Delete paused scheduler jobs
             - Delete detached subscriptions
             Old infrastructure is left alone. Topics are kept (passive, cheap).

There is no manifest file. Each command re-reads config.yml and identifies
resources by the `medallion-repo` label and the `<repo_name>-` name prefix.
This means: if you edit config.yml between prepare and activate, activate
will act on the EDITED config — which can be surprising. Re-run prepare
after any config change.

Assumptions:
  - ./src/validate_config.py exits 0 on success, non-zero on failure.
  - Dockerfile at repo root takes _PROCESSOR_NAME as a Cloud Build substitution.
  - GCP credentials at ./gcp-creds.json (or via GOOGLE_APPLICATION_CREDENTIALS).
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import subprocess
import sys
from pathlib import Path

import yaml
from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1, run_v2, scheduler_v1
from google.oauth2 import service_account

from medallion.fleet.pipeline_graph_model import (
    Extractor,
    PipelineGraph,
    Schedule,
    Transformer,
)

log = logging.getLogger("medallion.deploy")


# ---------------------------------------------------------------------------
# Config loading + naming
# ---------------------------------------------------------------------------


def load_config(path: Path) -> PipelineGraph:
    return PipelineGraph.model_validate(yaml.safe_load(path.read_text()))


def topic_name(repo_name: str, queue: str) -> str:
    return f"{repo_name}-{queue}"


def service_name(repo_name: str, processor: str) -> str:
    return f"{repo_name}-{processor}"


def subscription_name(repo_name: str, processor: str) -> str:
    return f"{repo_name}-{processor}"


def scheduler_job_name(repo_name: str, processor: str, schedule: str) -> str:
    return f"{repo_name}-{processor}-{schedule}"


def derive_queues(graph: PipelineGraph) -> list[str]:
    declared = {q.name for q in graph.queues}
    written = {p.writes_to for p in (*graph.extractors, *graph.transformers)}
    return sorted(declared | written)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def run_validation(config_path: Path, validator_path: Path) -> None:
    log.info("validating config via %s", validator_path)
    result = subprocess.run(
        [sys.executable, str(validator_path), str(config_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log.error("config validation failed:\n%s", result.stderr or result.stdout)
        raise SystemExit(f"validation failed (exit {result.returncode})")
    log.info("config valid")


# ---------------------------------------------------------------------------
# GCP clients
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class Clients:
    publisher: pubsub_v1.PublisherClient
    subscriber: pubsub_v1.SubscriberClient
    services: run_v2.ServicesClient
    scheduler: scheduler_v1.CloudSchedulerClient
    project: str
    region: str

    def topic_path(self, topic: str) -> str:
        return self.publisher.topic_path(self.project, topic)

    def subscription_path(self, sub: str) -> str:
        return self.subscriber.subscription_path(self.project, sub)

    def service_full_name(self, service: str) -> str:
        return f"{self.location_path()}/services/{service}"

    def scheduler_job_full_name(self, job: str) -> str:
        return f"{self.location_path()}/jobs/{job}"

    def location_path(self) -> str:
        return f"projects/{self.project}/locations/{self.region}"


def build_clients(creds_path: Path, project: str, region: str) -> Clients:
    creds = service_account.Credentials.from_service_account_file(str(creds_path))
    return Clients(
        publisher=pubsub_v1.PublisherClient(credentials=creds),
        subscriber=pubsub_v1.SubscriberClient(credentials=creds),
        services=run_v2.ServicesClient(credentials=creds),
        scheduler=scheduler_v1.CloudSchedulerClient(credentials=creds),
        project=project,
        region=region,
    )


def _scheduler_sa_email(project: str) -> str:
    return os.environ.get(
        "MEDALLION_SCHEDULER_SA",
        f"medallion-scheduler@{project}.iam.gserviceaccount.com",
    )


# ---------------------------------------------------------------------------
# Resource builders
# ---------------------------------------------------------------------------


def ensure_topic(clients: Clients, topic_name: str) -> None:
    """Topics are always live; nothing to make dormant. Idempotent."""
    topic = clients.topic_path(topic_name)
    try:
        clients.publisher.create_topic(request={"name": topic})
        log.info("created topic %s", topic_name)
    except gcp_exceptions.AlreadyExists:
        log.debug("topic exists: %s", topic_name)


def build_and_push_image(
    graph: PipelineGraph,
    processor: Extractor | Transformer,
    region: str,
    project: str,
) -> str:
    image = (
        f"{region}-docker.pkg.dev/{project}/medallion/"
        f"{graph.repo.name}-{processor.name}:latest"
    )
    log.info("building image %s", image)
    subprocess.run(
        [
            "gcloud",
            "builds",
            "submit",
            "--tag",
            image,
            "--substitutions",
            f"_PROCESSOR_NAME={processor.name}",
            "--quiet",
        ],
        check=True,
    )
    return image


def deploy_service(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor | Transformer,
    image: str,
    kind: str,
    *,
    dormant: bool,
) -> None:
    """Deploy a Cloud Run service.

    When dormant=True we deploy at 0/0 — the service definition is fully validated
    by Cloud Run (image pull, env, limits) so activation is just a scaling tweak.
    """
    svc_id = service_name(graph.repo.name, processor.name)
    full_name = clients.service_full_name(svc_id)
    runtime = graph.effective_runtime(processor)

    env = [
        run_v2.EnvVar(name="MEDALLION_REPO", value=graph.repo.name),
        run_v2.EnvVar(name="PROCESSOR_NAME", value=processor.name),
        run_v2.EnvVar(name="PROCESSOR_CLASS", value=processor.class_),
        run_v2.EnvVar(name="PROCESSOR_KIND", value=kind),
        run_v2.EnvVar(
            name="WRITE_TOPIC",
            value=clients.topic_path(topic_name(graph.repo.name, processor.writes_to)),
        ),
    ]
    if isinstance(processor, Transformer):
        env.append(
            run_v2.EnvVar(
                name="READ_TOPIC",
                value=clients.topic_path(
                    topic_name(graph.repo.name, processor.reads_from)
                ),
            )
        )

    timeout_seconds = int(runtime.timeout.rstrip("s"))
    min_inst = 0 if dormant else runtime.min_instances
    max_inst = 0 if dormant else runtime.max_instances

    container = run_v2.Container(
        image=image,
        env=env,
        resources=run_v2.ResourceRequirements(
            limits={"cpu": runtime.cpu, "memory": runtime.memory},
        ),
    )
    template = run_v2.RevisionTemplate(
        containers=[container],
        timeout={"seconds": timeout_seconds},
        max_instance_request_concurrency=runtime.concurrency,
        scaling=run_v2.RevisionScaling(
            min_instance_count=min_inst,
            max_instance_count=max_inst,
        ),
        labels={"medallion-repo": graph.repo.name, "medallion-kind": kind},
    )
    service = run_v2.Service(
        template=template,
        labels={"medallion-repo": graph.repo.name},
    )

    try:
        clients.services.get_service(name=full_name)
        service.name = full_name
        op = clients.services.update_service(service=service)
        log.info("updating service %s (dormant=%s)", svc_id, dormant)
    except gcp_exceptions.NotFound:
        op = clients.services.create_service(
            parent=clients.location_path(),
            service=service,
            service_id=svc_id,
        )
        log.info("creating service %s (dormant=%s)", svc_id, dormant)

    op.result()


def scale_service_live(
    clients: Clients, graph: PipelineGraph, processor: Extractor | Transformer
) -> None:
    """Activate: scale a 0/0 service to its configured min/max."""
    svc_id = service_name(graph.repo.name, processor.name)
    full_name = clients.service_full_name(svc_id)
    runtime = graph.effective_runtime(processor)
    service = clients.services.get_service(name=full_name)
    service.template.scaling = run_v2.RevisionScaling(
        min_instance_count=runtime.min_instances,
        max_instance_count=runtime.max_instances,
    )
    clients.services.update_service(service=service).result()
    log.info(
        "scaled service %s to %d/%d",
        svc_id,
        runtime.min_instances,
        runtime.max_instances,
    )


# --- subscriptions ----------------------------------------------------------
#
# Pub/Sub doesn't have a "paused" flag on subscriptions, but `detach_subscription`
# does what we need: the subscription exists, push config is preserved, but
# delivery stops and acks are rejected. `update_subscription` re-attaches by
# setting push_config again. Messages published while detached remain on the
# topic (subject to the topic's message retention) and are delivered after
# attach. That's exactly the handoff we want.


def ensure_subscription(
    clients: Clients,
    graph: PipelineGraph,
    processor: Transformer,
    service_url: str,
    *,
    dormant: bool,
) -> None:
    topic = clients.topic_path(topic_name(graph.repo.name, processor.reads_from))
    sub_id = subscription_name(graph.repo.name, processor.name)
    sub_path = clients.subscription_path(sub_id)
    runtime = graph.effective_runtime(processor)
    push_config = pubsub_v1.types.PushConfig(
        push_endpoint=f"{service_url}/run",
        oidc_token=pubsub_v1.types.PushConfig.OidcToken(
            service_account_email=_scheduler_sa_email(clients.project),
            audience=service_url,
        ),
    )

    try:
        clients.subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic,
                "push_config": push_config,
                "ack_deadline_seconds": min(600, int(runtime.timeout.rstrip("s"))),
                "labels": {"medallion-repo": graph.repo.name},
                "enable_message_ordering": True,
            }
        )
        log.info("created subscription %s", sub_id)
    except gcp_exceptions.AlreadyExists:
        sub = pubsub_v1.types.Subscription(name=sub_path, push_config=push_config)
        clients.subscriber.update_subscription(
            request={
                "subscription": sub,
                "update_mask": {"paths": ["push_config"]},
            }
        )
        log.info("updated subscription %s", sub_id)

    if dormant:
        clients.subscriber.detach_subscription(request={"subscription": sub_path})
        log.info("detached subscription %s (dormant)", sub_id)


def attach_subscription(
    clients: Clients, graph: PipelineGraph, processor: Transformer, service_url: str
) -> None:
    """Activate: re-attach a detached subscription by re-setting its push config."""
    sub_id = subscription_name(graph.repo.name, processor.name)
    sub_path = clients.subscription_path(sub_id)
    push_config = pubsub_v1.types.PushConfig(
        push_endpoint=f"{service_url}/run",
        oidc_token=pubsub_v1.types.PushConfig.OidcToken(
            service_account_email=_scheduler_sa_email(clients.project),
            audience=service_url,
        ),
    )
    sub = pubsub_v1.types.Subscription(name=sub_path, push_config=push_config)
    clients.subscriber.update_subscription(
        request={
            "subscription": sub,
            "update_mask": {"paths": ["push_config"]},
        }
    )
    log.info("attached subscription %s", sub_id)


# --- scheduler --------------------------------------------------------------


def ensure_schedule(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor,
    schedule: Schedule,
    service_url: str,
    *,
    dormant: bool,
) -> None:
    job_id = scheduler_job_name(graph.repo.name, processor.name, schedule.name)
    job_full = clients.scheduler_job_full_name(job_id)
    job = scheduler_v1.Job(
        name=job_full,
        schedule=schedule.cron,
        time_zone=schedule.timezone,
        http_target=scheduler_v1.HttpTarget(
            uri=f"{service_url}/run",
            http_method=scheduler_v1.HttpMethod.POST,
            oidc_token=scheduler_v1.OidcToken(
                service_account_email=_scheduler_sa_email(clients.project),
                audience=service_url,
            ),
        ),
    )
    try:
        clients.scheduler.create_job(parent=clients.location_path(), job=job)
        log.info("created schedule %s (%s)", job_id, schedule.cron)
    except gcp_exceptions.AlreadyExists:
        clients.scheduler.update_job(job=job)
        log.info("updated schedule %s (%s)", job_id, schedule.cron)

    if dormant:
        clients.scheduler.pause_job(name=job_full)
        log.info("paused schedule %s (dormant)", job_id)


def resume_schedule(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor,
    schedule: Schedule,
) -> None:
    job_id = scheduler_job_name(graph.repo.name, processor.name, schedule.name)
    clients.scheduler.resume_job(name=clients.scheduler_job_full_name(job_id))
    log.info("resumed schedule %s", job_id)


# ---------------------------------------------------------------------------
# Phase: PREPARE
# ---------------------------------------------------------------------------


def cmd_prepare(args, clients: Clients) -> None:
    run_validation(args.config, args.validator)
    graph = load_config(args.config)
    log.info("preparing repo=%s (services will be dormant)", graph.repo.name)

    # Topics first — services and subscriptions reference them.
    for queue in derive_queues(graph):
        ensure_topic(clients, topic_name(graph.repo.name, queue))

    # Build images + deploy services at 0/0. We need the service URL before
    # creating subscriptions/schedules, so this pass must complete first.
    service_urls: dict[str, str] = {}
    for processor, kind in (
        *((e, "extractor") for e in graph.extractors),
        *((t, "transformer") for t in graph.transformers),
    ):
        image = build_and_push_image(graph, processor, args.region, args.project)
        deploy_service(clients, graph, processor, image, kind, dormant=True)
        svc = clients.services.get_service(
            name=clients.service_full_name(
                service_name(graph.repo.name, processor.name)
            ),
        )
        service_urls[processor.name] = svc.uri

    # Triggers, all dormant.
    for extractor in graph.extractors:
        url = service_urls[extractor.name]
        for schedule in extractor.schedules or []:
            ensure_schedule(clients, graph, extractor, schedule, url, dormant=True)
    for transformer in graph.transformers:
        url = service_urls[transformer.name]
        ensure_subscription(clients, graph, transformer, url, dormant=True)

    log.info("prepare complete — run `activate` to flip live, `rollback` to discard")


# ---------------------------------------------------------------------------
# Phase: ACTIVATE
# ---------------------------------------------------------------------------


def cmd_activate(args, clients: Clients) -> None:
    # Don't re-validate here — prepare already did, and re-running gives a
    # false sense of safety if config.yml was edited between calls.
    graph = load_config(args.config)
    log.info("activating repo=%s", graph.repo.name)

    # Order: scale services up first, THEN attach triggers. If we attached
    # triggers first, scheduler/pubsub would hit services that still can't
    # serve, producing avoidable error logs.
    for processor in (*graph.extractors, *graph.transformers):
        scale_service_live(clients, graph, processor)

    for extractor in graph.extractors:
        for schedule in extractor.schedules or []:
            resume_schedule(clients, graph, extractor, schedule)
    for transformer in graph.transformers:
        url = clients.services.get_service(
            name=clients.service_full_name(
                service_name(graph.repo.name, transformer.name)
            ),
        ).uri
        attach_subscription(clients, graph, transformer, url)

    # New infra is now serving — safe to deactivate orphans.
    deactivate_orphans(clients, graph)
    log.info("activate complete")


def deactivate_orphans(clients: Clients, graph: PipelineGraph) -> None:
    """Tear down resources from this repo that aren't in the new config."""
    repo = graph.repo.name
    prefix = f"{repo}-"
    all_processors = (*graph.extractors, *graph.transformers)
    expected_services = {service_name(repo, p.name) for p in all_processors}
    expected_subs = {subscription_name(repo, t.name) for t in graph.transformers}
    expected_jobs = {
        scheduler_job_name(repo, e.name, s.name)
        for e in graph.extractors
        for s in (e.schedules or [])
    }

    for job in clients.scheduler.list_jobs(parent=clients.location_path()):
        job_id = job.name.rsplit("/", 1)[-1]
        if job_id.startswith(prefix) and job_id not in expected_jobs:
            clients.scheduler.delete_job(name=job.name)
            log.info("deleted orphan schedule %s", job_id)

    for sub in clients.subscriber.list_subscriptions(
        request={"project": f"projects/{clients.project}"},
    ):
        sub_id = sub.name.rsplit("/", 1)[-1]
        if sub_id.startswith(prefix) and sub_id not in expected_subs:
            clients.subscriber.delete_subscription(subscription=sub.name)
            log.info("deleted orphan subscription %s", sub_id)

    # Services: scale to 0/0 rather than delete, so a quick re-prepare/activate
    # cycle can resurrect them. Separate cleanup job prunes long-parked services.
    for service in clients.services.list_services(parent=clients.location_path()):
        svc_id = service.name.rsplit("/", 1)[-1]
        if (
            svc_id.startswith(prefix)
            and svc_id not in expected_services
            and service.labels.get("medallion-repo") == repo
        ):
            service.template.scaling = run_v2.RevisionScaling(
                min_instance_count=0,
                max_instance_count=0,
            )
            clients.services.update_service(service=service).result()
            log.info("parked orphan service %s", svc_id)


# ---------------------------------------------------------------------------
# Phase: ROLLBACK
# ---------------------------------------------------------------------------


def cmd_rollback(args, clients: Clients) -> None:
    """Delete prepared-but-not-activated resources. Old infra is untouched.

    We identify "prepared" resources by: in current config + dormant state
    (services at 0/0, paused jobs, detached subs). If a resource has already
    been activated, we leave it alone — rollback is for undoing prepare, not
    for tearing down live infra.
    """
    graph = load_config(args.config)
    repo = graph.repo.name
    log.info("rolling back prepared resources for repo=%s", repo)

    # Scheduler jobs: delete the paused ones in current config.
    for extractor in graph.extractors:
        for schedule in extractor.schedules or []:
            job_id = scheduler_job_name(repo, extractor.name, schedule.name)
            full = clients.scheduler_job_full_name(job_id)
            try:
                job = clients.scheduler.get_job(name=full)
                if job.state == scheduler_v1.Job.State.PAUSED:
                    clients.scheduler.delete_job(name=full)
                    log.info("deleted paused schedule %s", job_id)
                else:
                    log.info(
                        "skipping schedule %s (state=%s, not paused)",
                        job_id,
                        job.state.name,
                    )
            except gcp_exceptions.NotFound:
                pass

    # Subscriptions: detached subs have .detached == True. Delete those.
    for transformer in graph.transformers:
        sub_id = subscription_name(repo, transformer.name)
        sub_path = clients.subscription_path(sub_id)
        try:
            sub = clients.subscriber.get_subscription(subscription=sub_path)
            if sub.detached:
                clients.subscriber.delete_subscription(subscription=sub_path)
                log.info("deleted detached subscription %s", sub_id)
            else:
                log.info(
                    "skipping subscription %s (attached, not rolling back)", sub_id
                )
        except gcp_exceptions.NotFound:
            pass

    # Services: delete those at 0/0 — that's our dormant signature.
    for processor in (*graph.extractors, *graph.transformers):
        svc_id = service_name(repo, processor.name)
        full = clients.service_full_name(svc_id)
        try:
            svc = clients.services.get_service(name=full)
            scaling = svc.template.scaling
            if scaling.min_instance_count == 0 and scaling.max_instance_count == 0:
                clients.services.delete_service(name=full).result()
                log.info("deleted dormant service %s", svc_id)
            else:
                log.info(
                    "skipping service %s (scaled %d/%d, not dormant)",
                    svc_id,
                    scaling.min_instance_count,
                    scaling.max_instance_count,
                )
        except gcp_exceptions.NotFound:
            pass

    # Topics: left alone. Old infrastructure may still be using them, and
    # they're passive enough that there's no harm in keeping them around.
    log.info("rollback complete")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy a Medallion repository to GCP."
    )
    parser.add_argument("--config", type=Path, default=Path("config.yml"))
    parser.add_argument(
        "--validator", type=Path, default=Path("src/validate_config.py")
    )
    parser.add_argument("--creds", type=Path, default=Path("gcp-creds.json"))
    parser.add_argument("--project", default=os.environ.get("GCP_PROJECT"))
    parser.add_argument(
        "--region", default=os.environ.get("GCP_REGION", "europe-west1")
    )

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare", help="Build infra in dormant state")
    sub.add_parser("activate", help="Flip prepared infra live, deactivate orphans")
    sub.add_parser("rollback", help="Tear down prepared-but-not-activated infra")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    if not args.project:
        raise SystemExit("--project is required (or set GCP_PROJECT)")
    if not args.creds.exists():
        raise SystemExit(f"credentials not found at {args.creds}")

    clients = build_clients(args.creds, args.project, args.region)

    handlers = {
        "prepare": cmd_prepare,
        "activate": cmd_activate,
        "rollback": cmd_rollback,
    }
    handlers[args.command](args, clients)
    return 0


if __name__ == "__main__":
    sys.exit(main())
