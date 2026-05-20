from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager

import pendulum


from medallion.log import create_logger
from medallion.model.extractor import BaseExtractor
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_extractor_from_env
from medallion.store.store import must_get_env
from medallion.stream import Producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load config, init publisher client once
    app.state.processor = load_extractor_from_env()
    app.state.queue_producer = PubSubQueue(
        project_id=must_get_env("PUBSUB_PROJECT_ID"),
        topic_id=must_get_env("MEDALLION_TOPIC"),
        logger=create_logger(),
    )
    app.state.logger = create_logger()

    yield
    await app.state.processor.close()


app = FastAPI(lifespan=lifespan)
ARG_EXECUTION_START_TIME = "execution_start_time"
ARG_PREVIOUS_STEPS = "previous_steps"
ARG_IS_CHUNK_END = "is_chunk_end"
ORDERING_KEY_SEPARATOR = "|"


def ordering_key_from_steps(steps: list[str]) -> str:
    """Join ARG_PREVIOUS_STEPS into a stable, Pub/Sub-safe ordering key."""
    return ORDERING_KEY_SEPARATOR.join(steps)


def extract_and_publish_background(
    processor: BaseExtractor,
    producer: Producer,
) -> Callable[
    [],
    None,
]:
    def task():
        start_time = pendulum.now().format("YYYY-MM-DD_HH-mm-ssSSS")
        data = iter(processor.extract())

        try:
            current_item = next(data)
        except StopIteration:
            return

        with ThreadPoolExecutor(max_workers=4) as executor:
            for next_item in data:
                args = {
                    ARG_EXECUTION_START_TIME: start_time,
                    ARG_PREVIOUS_STEPS: [
                        processor.name,
                    ],
                    ARG_IS_CHUNK_END: False,
                }
                executor.submit(
                    producer.publish,
                    processor.write_output(current_item).getvalue(),
                    args,
                    ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
                )
                current_item = next_item

            args = {
                ARG_EXECUTION_START_TIME: start_time,
                ARG_PREVIOUS_STEPS: [
                    processor.name,
                ],
                ARG_IS_CHUNK_END: True,
            }
            executor.submit(
                producer.publish,
                processor.write_output(current_item).getvalue(),
                args,
                ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
            )

    return task


@app.post("/")
async def trigger(background: BackgroundTasks):
    background.add_task(
        extract_and_publish_background(
            app.state.processor,
            app.state.queue_producer,
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
