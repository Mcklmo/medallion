from logging import Logger
from medallion.log import create_logger
from medallion.model.extractor import is_force_extractor_run_enabled
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

    pipe = load_classes(
        store_output=store_output,
        store_cache=initialize_storage(
            must_get_env("LOCAL_CACHE_DIR"),
            logger,
        ),
        logger=logger,
        class_names=user_input_classes,
    )
    force_run_extractor = is_force_extractor_run_enabled()

    output_previous = pipe.run(force_run_extractor=force_run_extractor)


def main() -> None:
    medallion(create_logger())


if __name__ == "__main__":
    main()
