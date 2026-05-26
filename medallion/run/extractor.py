from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager


from medallion.log import create_logger
from medallion.model.extractor import (
    ARG_PREVIOUS_STEPS,
    LOCAL_OUTPUT_DIR_ENV_VAR,
    ORDERING_KEY_SEPARATOR,
    BaseExtractor,
    Streamer,
)
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_extractor_from_env
from medallion.store.base import BlobStore, must_get_env
from medallion.store.initialize_storage import initialize_storage
from medallion.queue.base import QueueWriter


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


def extract_and_stream(
    extractor: BaseExtractor,
    queue_writer: QueueWriter,
    store: BlobStore,
) -> Callable[
    [],
    None,
]:
    def task():
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
async def trigger(background: BackgroundTasks):
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
