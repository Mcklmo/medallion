from medallion.log import create_logger
from medallion.model.extractor import ARG_EXECUTION_START_TIME, ARG_IS_CHUNK_END
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_transformer_from_env
from medallion.model.transformer import BaseStreamingTransformer, BaseTransformer
from medallion.run.listener import Listener
from medallion.run.extractor import (
    ARG_PREVIOUS_STEPS,
    ordering_key_from_steps,
)
from medallion.store.store import must_get_env
from medallion.stream import Queue
from pydantic import Field


class TransformerListener(Listener):
    transformer: BaseTransformer | BaseStreamingTransformer
    messages_out: Queue
    messages_hot_store: list[bytes] = Field(
        init=False,
        default_factory=list,
        description="Store for messages that are currently being processed. Used for non-streaming transformers (because they require to see all messages before producing output).",
    )

    def process_message(
        self,
        data: bytes,
        is_chunk_end: bool,
        start_time: float,
        previous_steps: list[str],
    ) -> None:
        transformer = self.transformer
        args = {
            ARG_EXECUTION_START_TIME: start_time,
            ARG_PREVIOUS_STEPS: previous_steps + [transformer.name],
            ARG_IS_CHUNK_END: is_chunk_end,
        }

        if isinstance(transformer, BaseStreamingTransformer):
            message_data = transformer.read_input_bytes(data)
            output_data = transformer.transform_one(message_data)
            output_bytes = transformer.write_output(output_data)

            self.messages_out.write(
                data=output_bytes.read(),
                args=args,
                ordering_key=ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
            )

            return

        if not is_chunk_end:
            self.messages_hot_store.append(data)
            return

        output_data = transformer.transform(self.messages_hot_store)
        output_bytes = transformer.write_output(output_data)

        self.messages_out.write(
            data=output_bytes.read(),
            args=args,
            ordering_key=ordering_key_from_steps(args[ARG_PREVIOUS_STEPS]),
        )
        self.messages_hot_store.clear()


if __name__ == "__main__":
    project_id = must_get_env("PUBSUB_PROJECT_ID")
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
            topic_id=must_get_env("MEDALLION_TOPIC"),
            logger=logger,
        ),
        dlq=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env("MEDALLION_DLQ_TOPIC"),
            logger=logger,
        ),
        max_retries=int(must_get_env("LISTENER_MAX_RETRIES")),
        logger=create_logger(),
    )
    listener.listen()
