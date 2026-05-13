from typing import Any

from medallion.base import BaseTransformer
from medallion.pipeline import PipeLine
from medallion.stream import MessageConsumer, Message
from pydantic import BaseModel, ConfigDict, Field
import signal
from logging import Logger


class Horde(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    pipelines: list[PipeLine]
    message_consumer: MessageConsumer
    logger: Logger
    queue_to_transformers: dict[str, list[BaseTransformer]] = Field(init=False)

    def run(self) -> None:
        shutdown = False

        def handle_signal(signum, frame):
            nonlocal shutdown
            shutdown = True
            self.logger.info("Shutdown requested")

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        with self.message_consumer as consumer:
            for message in consumer.messages():
                if shutdown:
                    break

                try:
                    self.process_message(message)
                    consumer.ack(message)
                except Exception:
                    self.logger.exception("Failed to process message")
                    consumer.nack(message)

    def process_message(self, message: Message) -> None:
        queue_name = message.queue_name
        transformers = self.queue_to_transformers.get(queue_name, [])

        if not transformers:
            self.logger.warning(f"No transformers found for queue {queue_name}")
            return

        for transformer in transformers:
            output_data = transformer.transform(message.data)
            output_bytes = transformer.write_output(output_data)

            self.message_consumer.publish(
                data=output_bytes.read(),
                queue_name=transformer.queue_to,
            )

    def model_post_init(self, context: Any) -> None:
        queue_to_transformers: dict[str, list[BaseTransformer]] = {}

        for pipe in self.pipelines:
            for transformer in pipe.transformers or []:
                queue_to_transformers.setdefault(
                    transformer.queue_from,
                    [],
                ).append(transformer)

        self.queue_to_transformers = queue_to_transformers
