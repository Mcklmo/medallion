from example.extract import Extractor
from example.transform import Transformer
from medallion.log import create_logger
from medallion.pipeline import PipeLine
from medallion.store.store import initialize_storage, must_get_env


def main():
    logger = create_logger()
    result = PipeLine(
        extractor=Extractor(),
        transformers=[
            Transformer(),
        ],
        logger=logger,
        store_output=initialize_storage(
            must_get_env("LOCAL_OUTPUT_DIR"),
            logger,
        ),
        store_cache=initialize_storage(
            must_get_env("LOCAL_CACHE_DIR"),
            logger,
        ),
    ).run()

    print(result)


if __name__ == "__main__":
    main()
