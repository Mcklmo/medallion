from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from fastapi import FastAPI, BackgroundTasks, Request
from contextlib import asynccontextmanager

from fastapi.responses import JSONResponse


from medallion.log import create_logger
from medallion.model.extractor import (
    ARG_PREVIOUS_STEPS,
    ORDERING_KEY_SEPARATOR,
    BaseExtractor,
    Streamer,
)
from medallion.queue.mock import MockQueue
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_extractor_from_env
from medallion.store.base import MEDALLION_TOPIC_ENV, BlobStore, must_get_env
from medallion.store.initialize_storage import (
    initialize_storage,
)
from medallion.queue.base import Queue, QueueWriter

API_KEY_ENV = "EXTRACTOR_API_KEY"
API_KEY_HEADER = "X-Extractor-Api-Key"
GOOGLE_CLOUD_PROJECT_ENV_VAR = "GOOGLE_CLOUD_PROJECT"
QUEUE_TYPE_ENV_VAR = "QUEUE_TYPE"
PUB_SUB_QUEUE_TYPE = "pubsub"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.api_key_value = must_get_env(API_KEY_ENV)
    # Load config, init publisher client once
    logger = create_logger()
    app.state.extractor = load_extractor_from_env(logger)
    app.state.queue_producer = initialize_queue()
    app.state.logger = logger
    app.state.store = initialize_storage(
        logger=logger,
    )

    yield


def initialize_queue() -> Queue:
    queue_type = must_get_env(QUEUE_TYPE_ENV_VAR)
    if queue_type == PUB_SUB_QUEUE_TYPE:
        return PubSubQueue(
            project_id=must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR),
            topic_id=must_get_env(MEDALLION_TOPIC_ENV),
            logger=create_logger(),
        )

    if queue_type == "mock":
        return MockQueue()

    raise ValueError(f"Unsupported queue type: {queue_type}")


app = FastAPI(lifespan=lifespan)


def ordering_key_from_steps(steps: list[str]) -> str:
    """Join ARG_PREVIOUS_STEPS into a stable, Pub/Sub-safe ordering key."""
    return ORDERING_KEY_SEPARATOR.join(steps)


def extract_and_stream(
    extractor: BaseExtractor,
    queue_writer: QueueWriter,
    store: BlobStore,
) -> Callable[
    [],
    None,
]:
    def task() -> None:
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures: list[Future] = []

            class QueueStreamer(Streamer):
                def stream(
                    self,
                    output_data: bytes,
                    args: dict[
                        str,
                        Any,
                    ],
                ) -> None:
                    future = executor.submit(
                        queue_writer.write,
                        output_data,
                        args,
                        ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
                    )

                    futures.append(future)

            try:
                extractor.stream_output(
                    store,
                    streamer=QueueStreamer(),
                )
            finally:
                # Surface any exceptions from the write tasks, even if
                # stream_output raised. Retrieving every result ensures a
                # failed write isn't silently swallowed.
                for future in futures:
                    future.result()

    return task


@app.post("/")
async def trigger(request: Request, background: BackgroundTasks):
    value = request.headers.get(API_KEY_HEADER)
    if value != app.state.api_key_value:
        app.state.logger.warning(
            "Received trigger request with invalid API key: %s", value
        )

        return JSONResponse(
            status_code=401,
            content={
                "status": "unauthorized",
            },
        )

    app.state.logger.info(
        "Received trigger request, starting extraction and publishing in background"
    )
    background.add_task(
        extract_and_stream(
            app.state.extractor,
            app.state.queue_producer,
            app.state.store,
        )
    )

    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
        },
    )


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
