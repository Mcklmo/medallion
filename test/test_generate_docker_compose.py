from pathlib import Path

from medallion.configure_entrypoint.generate_docker_compose import LOG_VOLUME, generate
from medallion.configure_entrypoint.pipeline_graph_model import PipelineGraph
from medallion.configure_entrypoint.vscode import load_config
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR

BLANK_CONFIG = (
    Path(__file__).parent.parent / "medallion/example/blank_project/config.yml"
)


def test_every_processor_service_gets_a_tty_and_the_log_folder(monkeypatch):
    monkeypatch.setenv(LISTENER_MAX_RETRIES_ENV_VAR, "0")
    # The blank project's classes are templates, not importable; skip resolving them.
    monkeypatch.setattr(PipelineGraph, "model_post_init", lambda self, context: None)

    services = generate(load_config(BLANK_CONFIG))["services"]

    processors = {k: v for k, v in services.items() if k not in ("pubsub", "bootstrap")}
    assert processors
    assert all(svc["tty"] is True for svc in processors.values())
    assert all(LOG_VOLUME in svc["volumes"] for svc in processors.values())
