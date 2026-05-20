from example.streaming.extract import Extractor
from example.streaming.transform import Transformer
from medallion.consumer.mock import MockConsumer
from medallion.horde import Horde
from medallion.log import create_logger
from medallion.store.store import initialize_storage, must_get_env


def main():
    logger = create_logger()
    result = Horde(
        extractors=[
            Extractor(),
        ],
        logger=logger,
        message_consumer=MockConsumer(
            messages=[
                (
                    b"",
                    {"extractor_name": Extractor.__name__},
                    "",
                )
            ],
        ),
    ).run()

    print(result)


if __name__ == "__main__":
    main()
