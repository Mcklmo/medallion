from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any, Callable

from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager


from medallion.log import create_logger
from medallion.model.extractor import (
    ARG_PREVIOUS_STEPS,
    LOCAL_OUTPUT_DIR_ENV_VAR,
    ORDERING_KEY_SEPARATOR,
    BaseExtractor,
)
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_extractor_from_env
from medallion.store.base import BlobStore
from medallion.store.store import initialize_storage, must_get_env
from medallion.stream import QueueWriter


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load config, init publisher client once
    logger = create_logger()
    app.state.extractor = load_extractor_from_env(logger)
    app.state.queue_producer = PubSubQueue(
        project_id=must_get_env("PUBSUB_PROJECT_ID"),
        topic_id=must_get_env("MEDALLION_TOPIC"),
        logger=create_logger(),
    )
    app.state.logger = logger
    app.state.store = initialize_storage(
        output_dir=must_get_env(LOCAL_OUTPUT_DIR_ENV_VAR),
        logger=logger,
    )

    yield


app = FastAPI(lifespan=lifespan)


def ordering_key_from_steps(steps: list[str]) -> str:
    """Join ARG_PREVIOUS_STEPS into a stable, Pub/Sub-safe ordering key."""
    return ORDERING_KEY_SEPARATOR.join(steps)


def extract_and_publish_background(
    extractor: BaseExtractor,
    queue_writer: QueueWriter,
    store: BlobStore,
    logger: Logger,
) -> Callable[
    [],
    None,
]:
    def task():
        with ThreadPoolExecutor(max_workers=4) as executor:

            def write_to_queue(
                output_data: bytes,
                args: dict[
                    str,
                    Any,
                ],
            ) -> None:
                return executor.submit(
                    queue_writer.write,
                    output_data,
                    args,
                    ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
                )

            extractor.stream_output(
                data=extractor.load_or_extract_data(
                    logger,
                    store,
                ),
                stream_message_bytes=write_to_queue,
            )

    return task


@app.post("/")
async def trigger(background: BackgroundTasks):
    app.state.logger.info(
        "Received trigger request, starting extraction and publishing in background"
    )
    background.add_task(
        extract_and_publish_background(
            app.state.extractor,
            app.state.queue_producer,
            app.state.store,
            app.state.logger,
        )
    )

    return {"status": "accepted"}


@app.get("/healthz")
async def health():
    return {"ok": True}


if __name__ == "__main__":
    import os
    import uvicorn

    uvicorn.run(
        "medallion.run.extractor:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8080")),
    )
