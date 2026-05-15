from medallion.fleet.pipeline_graph_model import PipelineGraph
from medallion.resolve_classes import resolve_user_package

import yaml


def configure_runtime():
    package_name = resolve_user_package()

    with open(f"{package_name}/config.yml") as f:
        raw = yaml.safe_load(f)

    pipeline = PipelineGraph.model_validate(raw)


if __name__ == "__main__":
    configure_runtime()
