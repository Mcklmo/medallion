from logging import Logger
from medallion.log import create_logger
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.resolve_classes import (
    get_user_input,
    load_classes,
)
from medallion.store.store import initialize_storage, must_get_env


def medallion(logger: Logger) -> None:
    user_input_classes = get_user_input()
    store_output = initialize_storage(
        must_get_env("LOCAL_OUTPUT_DIR"),
        logger,
    )

    classes = load_classes(
        logger=logger,
        class_names=user_input_classes,
    )
    assert len(classes) >= 1, "At least an extractor class must be provided"
    transformer_instances = [c() for c in classes[1:]] if len(classes) > 1 else None
    pipe = PipeLine(
        queues=[
            MockQueue(messages=[]) for _ in range(len(transformer_instances or []) + 1)
        ],
        extractor=classes[0](),
        transformers=transformer_instances,
        store_output=store_output,
        store_cache=initialize_storage(
            must_get_env("LOCAL_CACHE_DIR"),
            logger,
        ),
        logger=logger,
    )

    pipe.run()


def main() -> None:
    medallion(create_logger())


if __name__ == "__main__":
    main()
