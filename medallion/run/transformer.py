from io import BytesIO
from medallion.log import create_logger
from medallion.model.extractor import (
    ARG_EXECUTION_START_TIME,
    ARG_IS_CHUNK_END,
    ARG_ITEM_INDEX,
)
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_transformer_from_env
from medallion.model.transformer import BaseStreamingTransformer, BaseTransformer
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR, Listener
from medallion.run.extractor import (
    ARG_PREVIOUS_STEPS,
    GOOGLE_CLOUD_PROJECT_ENV_VAR,
    ordering_key_from_steps,
)
from medallion.store.base import BlobStore, must_get_env, MEDALLION_TOPIC_ENV
from medallion.queue.base import Queue
from pydantic import BaseModel, Field

from medallion.store.initialize_storage import initialize_storage

FORCE_RUN_TRANSFORMER_ENV_VAR = "FORCE_RUN_TRANSFORMER"


def is_force_transformer_run_enabled():
    return must_get_env(FORCE_RUN_TRANSFORMER_ENV_VAR).lower() == "true"


class Args(BaseModel):
    execution_start_time: str = Field(alias=ARG_EXECUTION_START_TIME)
    previous_steps: list[str] = Field(alias=ARG_PREVIOUS_STEPS)
    is_chunk_end: bool = Field(alias=ARG_IS_CHUNK_END)
    item_index: int = Field(alias=ARG_ITEM_INDEX)


class TransformerListener(Listener):
    transformer: BaseTransformer | BaseStreamingTransformer
    messages_out: Queue
    messages_hot_store: list[bytes] = Field(
        init=False,
        default_factory=list,
        description="Store for messages that are currently being processed. Used for non-streaming transformers (because they require to see all messages before producing output).",
    )
    store: BlobStore
    force_run_transformer: bool

    def _after_listen(self) -> None:
        self.messages_out.close()

    def process_message(
        self,
        data: list[bytes],
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
    ) -> None:
        transformer = self.transformer
        args = Args(
            execution_start_time=start_time,
            previous_steps=previous_steps + [transformer.name],
            is_chunk_end=is_chunk_end,
            item_index=item_index,
        )

        if isinstance(transformer, BaseStreamingTransformer):
            message_data = transformer.read_input_bytes(data)
            output_data = transformer.load_cache_or_run(
                self.store,
                self.force_run_transformer,
                transformer.name,
                message_data,
            )
            output_bytes = transformer.write_output(output_data)
            assert isinstance(output_bytes, BytesIO)

            self.messages_out.write(
                data=output_bytes.read(),
                args=args.model_dump(),
                ordering_key=ordering_key_from_steps(args.previous_steps),
            )

            return

        self.messages_hot_store.append(data)

        if not is_chunk_end:
            return

        input_data = [transformer.read_input_bytes(d) for d in self.messages_hot_store]

        output_data = transformer.load_cache_or_run(
            self.store,
            self.force_run_transformer,
            transformer.name,
            input_data,
        )
        output_bytes = transformer.write_output(output_data)
        assert isinstance(output_bytes, BytesIO)

        self.messages_out.write(
            data=output_bytes.read(),
            args=args.model_dump(),
            ordering_key=ordering_key_from_steps(args.previous_steps),
        )
        self.messages_hot_store.clear()


if __name__ == "__main__":
    project_id = must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR)
    logger = create_logger()
    transformer = load_transformer_from_env(logger)
    store = initialize_storage(logger)
    listener = TransformerListener(
        transformer=transformer,
        messages_in=PubSubQueue(
            project_id=project_id,
            subscription_id=must_get_env("MEDALLION_SUBSCRIPTION"),
            logger=logger,
        ),
        messages_out=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env(MEDALLION_TOPIC_ENV),
            logger=logger,
        ),
        dlq=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env("MEDALLION_DLQ_TOPIC"),
            logger=logger,
        ),
        max_retries=int(must_get_env(LISTENER_MAX_RETRIES_ENV_VAR)),
        force_run_transformer=is_force_transformer_run_enabled(),
        logger=create_logger(),
        store=store,
    )
    listener.listen()
