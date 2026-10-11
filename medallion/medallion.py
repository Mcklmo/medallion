from logging import Logger
import yaml
from medallion.configure_entrypoint.pipeline_graph_model import (
    Extractor,
    Store,
    Transformer,
)
from medallion.configure_entrypoint.start_project import start_project
from medallion.configure_entrypoint.vscode import (
    DEFAULT_CONFIG_YAML_FILE_NAME,
    write_launch_json_file,
)
from medallion.log import create_logger, start_process_log
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.resolve_classes import (
    get_user_input,
    load_classes,
)
from medallion.store.base import must_get_env
from medallion.store.initialize_storage import initialize_storage
from medallion.model.store import BaseStore


def config_processor_names(class_names: list[str]) -> list[str]:
    """The config.yml names of the processors a local run chains, in run order;
    a class name where config.yml has no match."""
    if not DEFAULT_CONFIG_YAML_FILE_NAME.exists():
        return class_names

    # not PipelineGraph: validating it imports and checks every class in the config
    raw = yaml.safe_load(DEFAULT_CONFIG_YAML_FILE_NAME.read_text()) or {}
    extractors = [Extractor.model_validate(e) for e in raw.get("extractors") or []]
    downstream = [Transformer.model_validate(t) for t in raw.get("transformers") or []]
    downstream += [Store.model_validate(s) for s in raw.get("stores") or []]

    names: list[str] = []
    queue: str | None = None
    # one class can back several processors, so follow the queues from the extractor
    for i, class_name in enumerate(class_names):
        candidates = extractors if i == 0 else downstream
        processor = next(
            (
                p
                for p in candidates
                if p.class_ == class_name and getattr(p, "reads_from", None) == queue
            ),
            None,
        )
        names.append(class_name if processor is None else processor.name)
        queue = getattr(processor, "writes_to", None)

    return names


def main(
    logger: Logger = create_logger(),
) -> int:
    user_input = get_user_input()

    if user_input.vscode is not None:
        write_launch_json_file(include_all=user_input.vscode.include_all)
        return 0

    if user_input.new_project_name:
        start_project(user_input.new_project_name)
        return 0

    processor_names = config_processor_names(user_input.class_names)
    start_process_log(processor_names)

    store_output = initialize_storage(
        logger,
    )

    classes = load_classes(
        logger=logger,
        class_names=user_input.class_names,
    )
    if not classes:
        raise ValueError("At least an extractor class must be provided")

    transformer_and_store_instances = (
        [c(logger) for c in classes[1:]] if len(classes) > 1 else []
    )
    store_instances = [
        i
        for i in transformer_and_store_instances
        if isinstance(
            i,
            BaseStore,
        )
    ]
    assert len(store_instances) <= 1, "Only one store can be used in the pipeline"
    store = store_instances[0] if store_instances else None
    assert (
        store is None or store == transformer_and_store_instances[-1]
    ), "Store must be None or the last processor in the pipeline"

    transformer_instances = (
        transformer_and_store_instances
        if store is None
        else transformer_and_store_instances[:-1]
    )

    pipe = PipeLine(
        queues=[
            MockQueue(messages=[])
            for _ in range(len(transformer_and_store_instances or []) + 1)
        ],
        extractor=classes[0](logger),
        transformers=transformer_instances,
        store=store,
        store_output=store_output,
        logger=logger,
        processor_names=processor_names,
        force_run_transformer=must_get_env("FORCE_RUN_TRANSFORMER") == "true",
    )

    pipe.run()

    return 0


if __name__ == "__main__":
    main(create_logger())
