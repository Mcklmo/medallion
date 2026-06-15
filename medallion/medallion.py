from logging import Logger
from medallion.configure_entrypoint.start_project import start_project
from medallion.configure_entrypoint.vscode import write_launch_json_file
from medallion.log import create_logger
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.resolve_classes import (
    get_user_input,
    load_classes,
)
from medallion.store.base import get_env_or_default, must_get_env
from medallion.store.initialize_storage import initialize_storage


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

    store_output = initialize_storage(
        logger,
    )

    classes = load_classes(
        logger=logger,
        class_names=user_input.class_names,
    )
    if not classes:
        raise ValueError("At least an extractor class must be provided")

    transformer_instances = (
        [c(logger) for c in classes[1:]] if len(classes) > 1 else None
    )
    pipe = PipeLine(
        queues=[
            MockQueue(messages=[]) for _ in range(len(transformer_instances or []) + 1)
        ],
        extractor=classes[0](logger),
        transformers=transformer_instances,
        store_output=store_output,
        logger=logger,
        force_run_transformer=must_get_env("FORCE_RUN_TRANSFORMER") == "true",
    )

    pipe.run()

    return 0


if __name__ == "__main__":
    main(create_logger())
