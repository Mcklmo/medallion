from typing import Any, Iterable

from medallion.log import create_logger
from medallion.model.extractor import BaseExtractor
from medallion.queue.mock import MockQueue
from medallion.resolve_classes import load_extractor_from_env

from medallion.run.store import StorageListener
from medallion.store.base import BlobStore
from medallion.store.store import initialize_storage


def extract_and_format_messages(
    extractor: BaseExtractor,
) -> (
    Iterable[
        tuple[
            bytes,
            dict[
                str,
                Any,
            ],
        ],
    ]
    | None
):
    messages: Iterable[
        tuple[
            bytes,
            dict[
                str,
                Any,
            ],
        ]
    ] = []

    def collect_message(
        output_data: bytes,
        args: dict[
            str,
            Any,
        ],
    ) -> None:
        messages.append(
            (
                output_data,
                args,
            )
        )

    extractor.stream_output(
        data=extractor.load_or_extract_data(
            logger,
            store,
        ),
        stream_message_bytes=collect_message,
    )

    return messages if messages else None


if __name__ == "__main__":
    logger = create_logger()
    store: BlobStore = initialize_storage(
        logger,
    )
    extractor: BaseExtractor = load_extractor_from_env(logger)
    messages = extract_and_format_messages(
        extractor=extractor,
    )
    if messages is None:
        logger.info("Extractor did not yield any data, exiting")
        exit(0)

    listener = StorageListener(
        store=store,
        messages_in=MockQueue(
            messages=messages,
            block_when_empty=False,
        ),
        logger=logger,
        output_file_extension="jsonl",
    )
    listener.listen()
