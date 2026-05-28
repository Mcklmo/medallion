from example.basic.extract import Extractor
from example.basic.transform import Transformer
from medallion.log import create_logger
from medallion.pipeline import PipeLine
from medallion.store.base import must_get_env
from medallion.store.initialize_storage import initialize_storage


def main():
    logger = create_logger()
    result = PipeLine(
        extractor=Extractor(),
        transformers=[
            Transformer(),
        ],
        logger=logger,
        store_output=initialize_storage(
            logger,
        ),
        store_cache=initialize_storage(
            logger,
            local_output_dir=must_get_env("LOCAL_CACHE_DIR"),
        ),
    ).run()

    print(result)


if __name__ == "__main__":
    main()
