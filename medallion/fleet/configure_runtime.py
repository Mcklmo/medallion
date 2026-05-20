import json

import json5

import os

from medallion.fleet.pipeline_graph_model import PipelineGraph

import yaml


def configure_runtime():
    root = os.getcwd()
    with open(f"{root}/config.yml") as f:
        raw = yaml.safe_load(f)

    pipeline_graph = PipelineGraph.model_validate(raw)
    pipelines: list[list[str]] = pipeline_graph.get_pipeline_names()

    print("Pipelines to execute:")
    for i, pipeline in enumerate(pipelines):
        print(f"Pipeline {i+1}: {' -> '.join(pipeline)}")

    launch_json_path = ".vscode/launch.json"
    if not os.path.exists(launch_json_path):
        # create launch.json if it doesn't exist
        with open(launch_json_path, "w") as f:
            json.dump({"configurations": []}, f, indent=4)

    with open(launch_json_path) as f:
        launch_config = json5.load(f)  # json5 to support comments

    existing_configs = {config["name"] for config in launch_config["configurations"]}

    for i, pipeline in enumerate(pipelines):
        config_name = f"Run Pipeline: {' -> '.join(pipeline)}"
        if config_name in existing_configs:
            print(f"Launch configuration '{config_name}' already exists. Overwriting.")
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


if __name__ == "__main__":
    configure_runtime()
