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
from medallion.store.base import must_get_env, MEDALLION_TOPIC_ENV
from medallion.queue.base import Queue
from pydantic import BaseModel, Field


class Args(BaseModel):
    execution_start_time: str = Field(alias=ARG_EXECUTION_START_TIME)  # type: ignore[literal-required]
    previous_steps: list[str] = Field(alias=ARG_PREVIOUS_STEPS)  # type: ignore[literal-required]
    is_chunk_end: bool = Field(alias=ARG_IS_CHUNK_END)  # type: ignore[literal-required]
    item_index: int = Field(alias=ARG_ITEM_INDEX)  # type: ignore[literal-required]


class TransformerListener(Listener):
    transformer: BaseTransformer | BaseStreamingTransformer
    messages_out: Queue
    messages_hot_store: list[bytes] = Field(
        init=False,
        default_factory=list,
        description="Store for messages that are currently being processed. Used for non-streaming transformers (because they require to see all messages before producing output).",
    )

    def _after_listen(self) -> None:
        self.messages_out.close()

    def process_message(
        self,
        data: bytes,
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
            output_data = transformer.transform_one(message_data)
            output_bytes = transformer.write_output(output_data)
            assert isinstance(output_bytes, BytesIO)

            self.messages_out.write(
                data=output_bytes.read(),
                args=args.model_dump(),
                ordering_key=ordering_key_from_steps(args.previous_steps),
            )

            return

        if not is_chunk_end:
            self.messages_hot_store.append(data)
            return

        output_data = transformer.transform(self.messages_hot_store)
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
        logger=create_logger(),
    )
    listener.listen()
