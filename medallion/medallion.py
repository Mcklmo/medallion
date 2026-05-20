from logging import Logger
from medallion.log import create_logger
from medallion.resolve_classes import (
    get_user_input,
    load_classes,
)
from medallion.store.store import initialize_storage, must_get_env


def medallion(logger: Logger) -> None:
    user_input_classes, force_run_extractor = get_user_input()
    pipe = load_classes(
        store_output=initialize_storage(
            must_get_env("LOCAL_OUTPUT_DIR"),
            logger,
        ),
        store_cache=initialize_storage(
            must_get_env("LOCAL_CACHE_DIR"),
            logger,
        ),
        logger=logger,
        class_names=user_input_classes,
    )
    output_previous = pipe.run(force_run_extractor=force_run_extractor)

    print(output_previous)


def main() -> None:
    medallion(create_logger())


if __name__ == "__main__":
    main()
