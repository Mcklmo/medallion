from logging import Logger
from medallion.log import create_logger
from medallion.resolve_classes import (
    get_user_specified_class_names,
    load_classes,
)
from medallion.store.store import initialize_storage, must_get_env


def medallion(logger: Logger) -> None:
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
        class_names=get_user_specified_class_names(),
    )
    output_previous = pipe.run()

    print(output_previous)


def main() -> None:
    medallion(create_logger())


if __name__ == "__main__":
    main()
