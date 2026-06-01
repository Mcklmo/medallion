import json
from pathlib import Path

import json5
from pydantic import BaseModel, Field
import yaml

from medallion.configure_entrypoint.pipeline_graph_model import PipelineGraph
from medallion.log import create_logger
from medallion.resolve_classes import get_medallion_root

MEDALLION_ROOT = get_medallion_root()
DEFAULT_CONFIG_NAME = Path(f"{MEDALLION_ROOT}/config.yml")


def load_config(path: Path | None = None) -> PipelineGraph:
    if path is None:
        path = DEFAULT_CONFIG_NAME

    raw = yaml.safe_load(path.read_text())

    return PipelineGraph.model_validate(raw)


class VSCodeConfig(BaseModel):
    name: str
    type_: str = Field(
        default="debugpy",
        alias="type",
    )
    request: str = "launch"
    module: str | None = None
    connect: dict | None = None
    pathMappings: list[dict] | None = None
    justMyCode: bool = False
    args: list[str] | None = None


_default_configurations = [
    VSCodeConfig(
        name="Configure VS Code Debugger",
        module="medallion.medallion",
        args=["vscode"],
    ),
    VSCodeConfig(
        name="Run Extractor HTTP Server",
        module="uvicorn",
        args=[
            "medallion.run.extractor:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8080",
        ],
    ),
    VSCodeConfig(
        name="Generate docker compose",
        module="medallion.configure_entrypoint.generate_docker_compose",
    ),
    VSCodeConfig(
        name="Attach to docker service",
        request="attach",
        connect={"host": "localhost", "port": 5678},
        pathMappings=[
            {
                "localRoot": "${workspaceFolder}/medallion",
                "remoteRoot": "/app/medallion",
            },
            {
                "localRoot": MEDALLION_ROOT,
                "remoteRoot": "/app/src",
            },
        ],
    ),
]
default_configurations = [
    config.model_dump(
        by_alias=True,
        exclude_none=True,
    )
    for config in _default_configurations
]
logger = create_logger()


def write_launch_json_file(include_all: bool = False) -> None:
    pipeline_graph = load_config()
    pipelines: list[list[str]] = pipeline_graph.get_pipeline_names()

    logger.info("Pipelines to execute:")
    for i, pipeline in enumerate(pipelines):
        logger.info(f"Pipeline {i+1}: {' -> '.join(pipeline)}")

    launch_json_path = Path(MEDALLION_ROOT) / ".vscode" / "launch.json"
    if not launch_json_path.exists():
        # create launch.json if it doesn't exist
        launch_json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(launch_json_path, "w") as f:
            json.dump({"configurations": []}, f, indent=4)

    with open(launch_json_path) as f:
        launch_config = json5.load(f)  # json5 to support comments

    existing_configs = {config["name"] for config in launch_config["configurations"]}

    for default_config in default_configurations:
        if default_config["name"] not in existing_configs:
            logger.info(
                f"Adding default launch configuration '{default_config['name']}'."
            )
            launch_config["configurations"].append(default_config)

            continue

        logger.info(
            f"Default launch configuration '{default_config['name']}' already exists. Overwriting."
        )

        launch_config["configurations"] = [
            config if config["name"] != default_config["name"] else default_config
            for config in launch_config["configurations"]
        ]

    for i, pipeline in enumerate(pipelines):
        config_name = f"Run Pipeline: {' -> '.join(pipeline)}"
        if config_name in existing_configs:
            logger.info(
                f"Launch configuration '{config_name}' already exists. Overwriting."
            )
            launch_config["configurations"] = [
                config
                for config in launch_config["configurations"]
                if config["name"] != config_name
            ]

        new_config = {
            "name": config_name,
            "type": "debugpy",
            "request": "launch",
            "module": "medallion.medallion",
            "args": pipeline,
            "justMyCode": False,
        }
        launch_config["configurations"].append(new_config)

    with open(launch_json_path, "w") as f:
        json.dump(launch_config, f, indent=4)
