"""
Medallion deployment script — three-phase deploy.

Commands:
  prepare    Validate config, then build all infrastructure in a DORMANT state:
             - Topics created (always live; passive resource)
             - Cloud Run services deployed but scaled to 0/0 (cannot serve)
             - Pub/Sub pull subscriptions created and labelled `dormant`
             - Cloud Scheduler jobs created PAUSED (no triggers)
             Old infrastructure continues serving traffic untouched.

  activate   Re-read config.yml and flip prepared resources live:
             - Scale services to configured min/max
             - Label subscriptions as `active`
             - Resume scheduler jobs
             Then deactivate orphans (resources from this repo not in new config).

  rollback   Re-read config.yml and tear down prepared-but-not-activated resources:
             - Delete services that are still at 0/0
             - Delete paused scheduler jobs
             - Delete dormant-labelled subscriptions
             Old infrastructure is left alone. Topics are kept (passive, cheap).

There is no manifest file. Each command re-reads config.yml and identifies
resources by the `medallion-repo` label and the `<repo_name>-` name prefix.
This means: if you edit config.yml between prepare and activate, activate
will act on the EDITED config — which can be surprising. Re-run prepare
after any config change.

Assumptions:
  - Dockerfile at repo root takes _PROCESSOR_NAME as a Cloud Build substitution.
  - GCP credentials at ./gcp-creds.json (or via GOOGLE_APPLICATION_CREDENTIALS).
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import subprocess
import uuid
import sys
from pathlib import Path

from google.api_core import exceptions as gcp_exceptions
from google.cloud import pubsub_v1, run_v2, scheduler_v1, secretmanager
from google.oauth2 import service_account

from medallion.configure_entrypoint.configure_runtime import load_config
from medallion.configure_entrypoint.pipeline_graph_model import (
    Extractor,
    PipelineGraph,
    Schedule,
    Store,
    Transformer,
)
from medallion.configure_entrypoint.resource_names import (
    DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
    dlq_topic_name,
    scheduler_job_name,
    service_name,
    storage_subscription_name,
    topic_name,
    transformer_subscription_name,
)
from medallion.log import create_logger
from medallion.model.extractor import FORCE_RUN_EXTRACTOR_ENV_VAR
from medallion.run.extractor import (
    API_KEY_ENV,
    GOOGLE_CLOUD_PROJECT_ENV_VAR,
    PUB_SUB_QUEUE_TYPE,
    QUEUE_TYPE_ENV_VAR,
)
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR
from medallion.store.initialize_storage import (
    GCS_BUCKET_ENV_VAR,
)
from medallion.resolve_classes import (
    EXTRACTOR_CLASS_ENV_VAR,
    MEDALLION_ROOT_ENV,
    TRANSFORMER_CLASS_ENV_VAR,
)
from medallion.store.base import (
    FILE_STORAGE_TYPE_ENV_VAR,
    MEDALLION_TOPIC_ENV,
    must_get_env,
)

log = create_logger()


def derive_queues(graph: PipelineGraph) -> list[str]:
    declared = {q.name for q in graph.queues}
    written = {p.writes_to for p in (*graph.extractors, *graph.transformers)}
    return sorted(declared | written)


def stores_for_queues(graph: PipelineGraph) -> list[Store]:
    return [
        Store(name=f"store-{q.name}", class_="BaseStore", reads_from=q.name)
        for q in graph.queues
    ]


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


# ---------------------------------------------------------------------------
# Resource builders
# ---------------------------------------------------------------------------


def ensure_topic(clients: Clients, topic: str) -> None:
    """Topics are always live; nothing to make dormant. Idempotent."""
    path = clients.topic_path(topic)
    try:
        clients.publisher.create_topic(request={"name": path})
        log.info("created topic %s", topic)
    except gcp_exceptions.AlreadyExists:
        log.debug("topic exists: %s", topic)


def build_and_push_image(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor | Transformer,
    google_application_credentials_path: Path,
    repository: str,
) -> str:
    tag = uuid.uuid4().hex[:12]
    image = (
        f"{clients.region}-docker.pkg.dev/{clients.project}/{repository}/"
        f"{graph.repo.name}-{processor.name}:{tag}"
    )
    log.info("building image %s", image)

    # authenticate docker before build so buildx --push works
    auth_result = subprocess.run(
        [
            "docker",
            "login",
            "-u",
            "_json_key",
            "--password-stdin",
            f"https://{clients.region}-docker.pkg.dev",
        ],
        input=google_application_credentials_path.read_text(),
        check=False,
        capture_output=True,
        text=True,
    )
    if auth_result.returncode != 0:
        raise RuntimeError(f"docker authentication failed")

    result = subprocess.run(
        [
            "docker",
            "buildx",
            "build",
            "--platform",
            "linux/amd64",
            "--provenance=false",
            "-t",
            image,
            "--push",
            ".",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker push failed for {processor.name}:\n{result.stderr}")

    return image


secret_client = secretmanager.SecretManagerServiceClient()


def deploy_service(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor | Transformer | Store,
    image: str,
    kind: str,
    *,
    dormant: bool,
    dlq_topic: str | None = None,
) -> None:
    """Deploy a Cloud Run service.

    When dormant=True we deploy at 0/0 — the service definition is fully validated
    by Cloud Run (image pull, env, limits) so activation is just a scaling tweak.
    """
    svc_id = processor.name
    full_name = clients.service_full_name(svc_id)
    runtime = graph.effective_runtime(processor)

    env = [
        run_v2.EnvVar(name="MEDALLION_REPO", value=graph.repo.name),
        run_v2.EnvVar(name="PROCESSOR_NAME", value=processor.name),
        run_v2.EnvVar(name="PROCESSOR_KIND", value=kind),
        run_v2.EnvVar(name=GOOGLE_CLOUD_PROJECT_ENV_VAR, value=clients.project),
        run_v2.EnvVar(
            name=QUEUE_TYPE_ENV_VAR,
            value=PUB_SUB_QUEUE_TYPE,
        ),
        run_v2.EnvVar(
            name=MEDALLION_ROOT_ENV,
            value="/app/src",  # hard mapped in Dockerfile.
        ),
    ]
    if isinstance(
        processor,
        (
            Extractor,
            Store,
        ),
    ):
        env.extend(
            [
                run_v2.EnvVar(
                    name=FILE_STORAGE_TYPE_ENV_VAR,
                    value="gcs",
                ),
                run_v2.EnvVar(
                    name=GCS_BUCKET_ENV_VAR,
                    value=graph.repo.name,
                ),
                run_v2.EnvVar(
                    name=EXTRACTOR_CLASS_ENV_VAR,
                    value=processor.class_,
                ),
            ]
        )

    if isinstance(processor, (Transformer, Extractor)):
        pubsub_topic_name = topic_name(graph.repo.name, processor.writes_to)
        log.info(
            f"Adding Pub/Sub topic env var for {processor.name}: {pubsub_topic_name}"
        )
        env.append(
            run_v2.EnvVar(
                name=MEDALLION_TOPIC_ENV,
                value=pubsub_topic_name,
            )
        )

    if isinstance(
        processor,
        Extractor,
    ):
        env.append(
            run_v2.EnvVar(
                name=EXTRACTOR_CLASS_ENV_VAR,
                value=processor.class_,
            )
        )
        env.append(
            run_v2.EnvVar(
                name=FORCE_RUN_EXTRACTOR_ENV_VAR,
                value="true",
            )
        )
        api_key_secret = secret_client.access_secret_version(
            request={
                "name": f"projects/{clients.project}/secrets/{processor.name}-api-key/versions/latest",
            }
        )

        env.append(
            run_v2.EnvVar(
                name=API_KEY_ENV,
                value=api_key_secret.payload.data.decode("UTF-8"),
            )
        )

    if isinstance(
        processor,
        Transformer,
    ):
        env.extend(
            [
                run_v2.EnvVar(
                    name="READ_TOPIC",
                    value=clients.topic_path(
                        topic_name(graph.repo.name, processor.reads_from)
                    ),
                ),
                run_v2.EnvVar(
                    name=TRANSFORMER_CLASS_ENV_VAR,
                    value=processor.class_,
                ),
            ]
        )

    if isinstance(
        processor,
        (
            Transformer,
            Store,
        ),
    ):
        env.append(
            run_v2.EnvVar(
                name="MEDALLION_SUBSCRIPTION",
                value=(
                    transformer_subscription_name(graph.repo.name, processor.name)
                    if isinstance(processor, Transformer)
                    else storage_subscription_name(
                        graph.repo.name, processor.reads_from
                    )
                ),
            )
        )
        env.append(
            run_v2.EnvVar(
                name=LISTENER_MAX_RETRIES_ENV_VAR,
                value=must_get_env(LISTENER_MAX_RETRIES_ENV_VAR),
            )
        )

    if dlq_topic is not None:
        env.append(
            run_v2.EnvVar(name="MEDALLION_DLQ_TOPIC", value=dlq_topic),
        )

    timeout_seconds = int(runtime.timeout.rstrip("s"))
    min_inst = 0 if dormant else runtime.min_instances
    max_inst = 0 if dormant else runtime.max_instances

    container = run_v2.Container(
        image=image,
        command=["python", "-m", f"medallion.run.{kind}"],
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
        try:
            op = clients.services.create_service(
                parent=clients.location_path(),
                service=service,
                service_id=svc_id,
            )
        except Exception as e:
            log.error("failed to create service %s: %s", svc_id, e)
            raise

        log.info("creating service %s (dormant=%s)", svc_id, dormant)

    op.result()

    if isinstance(processor, Extractor):
        from google.iam.v1 import iam_policy_pb2, policy_pb2

        policy = clients.services.get_iam_policy(
            request=iam_policy_pb2.GetIamPolicyRequest(resource=full_name)
        )
        invoker_binding = None
        for binding in policy.bindings:
            if binding.role == "roles/run.invoker":
                invoker_binding = binding
                break
        if invoker_binding is None:
            policy.bindings.append(
                policy_pb2.Binding(
                    role="roles/run.invoker",
                    members=["allUsers"],
                )
            )
        elif "allUsers" not in invoker_binding.members:
            invoker_binding.members.append("allUsers")
        clients.services.set_iam_policy(
            request=iam_policy_pb2.SetIamPolicyRequest(
                resource=full_name,
                policy=policy,
            )
        )
        log.info("allowed unauthenticated requests for extractor %s", svc_id)


def scale_service_live(
    clients: Clients,
    graph: PipelineGraph,
    processor: Extractor | Transformer | Store,
) -> None:
    """Activate: scale a 0/0 service to its configured min/max."""
    svc_id = processor.name
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
# Transformers and stores use streaming pull (subscriber.subscribe()) — they
# are long-running Cloud Run services that actively pull messages, not HTTP
# endpoints. Subscriptions are therefore plain pull subscriptions with no
# push_config.
#
# Dormant lifecycle: we use subscription labels to track state. A label
# `medallion-state: dormant` marks a subscription created during `prepare`
# but not yet activated. `activate` updates the label to `active`. `rollback`
# deletes subscriptions labelled `dormant`. When the service is at 0/0
# (dormant), no instances are running to pull, so the subscription is
# effectively idle — messages accumulate on the topic subject to the topic's
# message retention policy and are consumed once the service scales up.


def ensure_subscription(
    clients: Clients,
    graph: PipelineGraph,
    processor: Transformer,
    *,
    dormant: bool,
    dlq_topic_path: str,
) -> None:
    topic = clients.topic_path(topic_name(graph.repo.name, processor.reads_from))
    sub_id = transformer_subscription_name(graph.repo.name, processor.name)
    sub_path = clients.subscription_path(sub_id)
    runtime = graph.effective_runtime(processor)
    state_label = "dormant" if dormant else "active"
    labels = {"medallion-repo": graph.repo.name, "medallion-state": state_label}

    try:
        clients.subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic,
                "ack_deadline_seconds": min(600, int(runtime.timeout.rstrip("s"))),
                "labels": labels,
                "enable_message_ordering": True,
                "dead_letter_policy": {
                    "dead_letter_topic": dlq_topic_path,
                    "max_delivery_attempts": DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
                },
            }
        )
        log.info("created subscription %s (state=%s)", sub_id, state_label)
    except gcp_exceptions.AlreadyExists:
        sub = pubsub_v1.types.Subscription(
            name=sub_path,
            labels=labels,
            dead_letter_policy=pubsub_v1.types.DeadLetterPolicy(
                dead_letter_topic=dlq_topic_path,
                max_delivery_attempts=DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
            ),
        )
        clients.subscriber.update_subscription(
            request={
                "subscription": sub,
                "update_mask": {
                    "paths": ["labels", "dead_letter_policy"],
                },
            }
        )
        log.info("updated subscription %s (state=%s)", sub_id, state_label)


def activate_subscription(
    clients: Clients, graph: PipelineGraph, processor: Transformer
) -> None:
    """Activate: flip a dormant subscription to active by updating its label."""
    sub_id = transformer_subscription_name(graph.repo.name, processor.name)
    sub_path = clients.subscription_path(sub_id)
    sub = pubsub_v1.types.Subscription(
        name=sub_path,
        labels={"medallion-repo": graph.repo.name, "medallion-state": "active"},
    )
    clients.subscriber.update_subscription(
        request={
            "subscription": sub,
            "update_mask": {"paths": ["labels"]},
        }
    )
    log.info("activated subscription %s", sub_id)


# --- storage subscriptions --------------------------------------------------


def ensure_storage_subscription(
    clients: Clients,
    graph: PipelineGraph,
    queue: str,
    *,
    dormant: bool,
    dlq_topic_path: str,
) -> None:
    topic = clients.topic_path(topic_name(graph.repo.name, queue))
    sub_id = storage_subscription_name(graph.repo.name, queue)
    sub_path = clients.subscription_path(sub_id)
    state_label = "dormant" if dormant else "active"
    labels = {"medallion-repo": graph.repo.name, "medallion-state": state_label}

    try:
        clients.subscriber.create_subscription(
            request={
                "name": sub_path,
                "topic": topic,
                "ack_deadline_seconds": 60,
                "labels": labels,
                "enable_message_ordering": True,
                "dead_letter_policy": {
                    "dead_letter_topic": dlq_topic_path,
                    "max_delivery_attempts": DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
                },
            }
        )
        log.info("created storage subscription %s (state=%s)", sub_id, state_label)
    except gcp_exceptions.AlreadyExists:
        sub = pubsub_v1.types.Subscription(
            name=sub_path,
            labels=labels,
            dead_letter_policy=pubsub_v1.types.DeadLetterPolicy(
                dead_letter_topic=dlq_topic_path,
                max_delivery_attempts=DEAD_LETTER_MAX_DELIVERY_ATTEMPTS,
            ),
        )
        clients.subscriber.update_subscription(
            request={
                "subscription": sub,
                "update_mask": {
                    "paths": ["labels", "dead_letter_policy"],
                },
            }
        )
        log.info("updated storage subscription %s (state=%s)", sub_id, state_label)


def activate_storage_subscription(
    clients: Clients,
    graph: PipelineGraph,
    queue: str,
) -> None:
    """Activate: flip a dormant storage subscription to active by updating its label."""
    sub_id = storage_subscription_name(graph.repo.name, queue)
    sub_path = clients.subscription_path(sub_id)
    sub = pubsub_v1.types.Subscription(
        name=sub_path,
        labels={"medallion-repo": graph.repo.name, "medallion-state": "active"},
    )
    clients.subscriber.update_subscription(
        request={
            "subscription": sub,
            "update_mask": {"paths": ["labels"]},
        }
    )
    log.info("activated storage subscription %s", sub_id)


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
                service_account_email=must_get_env("MEDALLION_SCHEDULER_SA"),
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


def cmd_prepare(
    args,
    clients: Clients,
) -> None:
    graph = load_config()
    stores = stores_for_queues(graph)
    log.info("preparing repo=%s (services will be dormant)", graph.repo.name)

    # Topics first — services and subscriptions reference them.
    for queue in derive_queues(graph):
        ensure_topic(clients, topic_name(graph.repo.name, queue))
        ensure_topic(clients, dlq_topic_name(graph.repo.name, queue))

    # Build images + deploy services at 0/0.
    for processor, kind in (
        *((e, "extractor") for e in graph.extractors),
        *((t, "transformer") for t in graph.transformers),
        *((s, "store") for s in stores),
    ):
        image = build_and_push_image(
            clients,
            graph,
            processor,
            args.creds,
            graph.repo.name,
        )
        dlq = (
            dlq_topic_name(graph.repo.name, processor.reads_from)
            if isinstance(processor, (Transformer, Store))
            else None
        )
        deploy_service(
            clients,
            graph,
            processor,
            image,
            kind,
            dormant=True,
            dlq_topic=dlq,
        )

    # Triggers, all dormant.
    for extractor in graph.extractors:
        svc = clients.services.get_service(
            name=clients.service_full_name(extractor.name),
        )
        for schedule in extractor.schedules or []:
            ensure_schedule(
                clients,
                graph,
                extractor,
                schedule,
                svc.uri,
                dormant=True,
            )
    for transformer in graph.transformers:
        ensure_subscription(
            clients,
            graph,
            transformer,
            dormant=True,
            dlq_topic_path=clients.topic_path(
                dlq_topic_name(graph.repo.name, transformer.reads_from)
            ),
        )
    for store in stores:
        ensure_storage_subscription(
            clients,
            graph,
            store.reads_from,
            dormant=True,
            dlq_topic_path=clients.topic_path(
                dlq_topic_name(graph.repo.name, store.reads_from)
            ),
        )

    log.info("prepare complete — run `activate` to flip live, `rollback` to discard")


# ---------------------------------------------------------------------------
# Phase: ACTIVATE
# ---------------------------------------------------------------------------


def cmd_activate(args, clients: Clients) -> None:
    # Don't re-validate here — prepare already did, and re-running gives a
    # false sense of safety if config.yml was edited between calls.
    graph = load_config()
    stores = stores_for_queues(graph)
    log.info("activating repo=%s", graph.repo.name)

    # Order: scale services up first, THEN activate triggers. If we activated
    # triggers first, scheduler would hit services that still can't serve,
    # producing avoidable error logs.
    for processor in (*graph.extractors, *graph.transformers, *stores):
        scale_service_live(clients, graph, processor)

    for extractor in graph.extractors:
        for schedule in extractor.schedules or []:
            resume_schedule(clients, graph, extractor, schedule)
    for transformer in graph.transformers:
        activate_subscription(clients, graph, transformer)
    for store in stores:
        activate_storage_subscription(clients, graph, store.reads_from)

    # New infra is now serving — safe to deactivate orphans.
    deactivate_orphans(clients, graph)
    log.info("activate complete")


def deactivate_orphans(clients: Clients, graph: PipelineGraph) -> None:
    """Tear down resources from this repo that aren't in the new config."""
    repo = graph.repo.name
    stores = stores_for_queues(graph)
    prefix = f"{repo}-"
    all_processors = (*graph.extractors, *graph.transformers, *stores)
    expected_services = {service_name(repo, p.name) for p in all_processors}
    expected_subs = {
        transformer_subscription_name(repo, t.name) for t in graph.transformers
    }
    expected_subs |= {storage_subscription_name(repo, q.name) for q in graph.queues}
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
    graph = load_config()
    stores = stores_for_queues(graph)
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

    # Subscriptions: dormant subs have medallion-state=dormant label. Delete those.
    for transformer in graph.transformers:
        sub_id = transformer_subscription_name(repo, transformer.name)
        sub_path = clients.subscription_path(sub_id)
        try:
            sub = clients.subscriber.get_subscription(subscription=sub_path)
            if sub.labels.get("medallion-state") == "dormant":
                clients.subscriber.delete_subscription(subscription=sub_path)
                log.info("deleted dormant subscription %s", sub_id)
            else:
                log.info(
                    "skipping subscription %s (state=%s, not dormant)",
                    sub_id,
                    sub.labels.get("medallion-state", "unknown"),
                )
        except gcp_exceptions.NotFound:
            pass

    # Storage subscriptions: same logic — delete if dormant.
    for queue in derive_queues(graph):
        sub_id = storage_subscription_name(repo, queue)
        sub_path = clients.subscription_path(sub_id)
        try:
            sub = clients.subscriber.get_subscription(subscription=sub_path)
            if sub.labels.get("medallion-state") == "dormant":
                clients.subscriber.delete_subscription(subscription=sub_path)
                log.info("deleted dormant storage subscription %s", sub_id)
            else:
                log.info(
                    "skipping storage subscription %s (state=%s, not dormant)",
                    sub_id,
                    sub.labels.get("medallion-state", "unknown"),
                )
        except gcp_exceptions.NotFound:
            pass

    # Services: delete those at 0/0 — that's our dormant signature.
    for processor in (*graph.extractors, *graph.transformers, *stores):
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

    # Topics (including DLQ topics): left alone. Old infrastructure may still
    # be using them, and they're passive enough that there's no harm in
    # keeping them around.
    log.info("rollback complete")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deploy a Medallion repository to GCP."
    )
    parser.add_argument(
        "--creds", type=Path, default=must_get_env("GOOGLE_APPLICATION_CREDENTIALS")
    )
    parser.add_argument("--project", default=must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR))
    parser.add_argument("--region", default=must_get_env("GCP_REGION"))

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
        raise SystemExit(
            f"--project is required (or set {GOOGLE_CLOUD_PROJECT_ENV_VAR})"
        )
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
