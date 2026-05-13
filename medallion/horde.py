from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from typing import Any

from medallion.base import BaseTransformer
from medallion.pipeline import PipeLine
from medallion.stream import Message, MessageConsumer
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
    max_concurrent_messages: int = 8
    queue_to_transformers: dict[str, list[BaseTransformer]] = Field(init=False)
    message_executor: ThreadPoolExecutor = Field(init=False)
    transformer_executor: ThreadPoolExecutor = Field(init=False)

    def run(self) -> None:
        shutdown = False

        def handle_signal(signum, frame):
            nonlocal shutdown
            shutdown = True
            self.logger.info("Shutdown requested")

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        try:
            with self.message_consumer as consumer:
                for message in consumer.messages():
                    if shutdown:
                        break

                    self.message_executor.submit(
                        self._handle_message,
                        consumer,
                        message,
                    )
        finally:
            self.message_executor.shutdown(wait=True)
            self.transformer_executor.shutdown(wait=True)

    def _handle_message(
        self,
        consumer: MessageConsumer,
        message: Message,
    ) -> None:
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

        futures = [
            self.transformer_executor.submit(
                self._run_transformer,
                transformer,
                message.data,
            )
            for transformer in transformers
        ]

        done, _ = wait(futures, return_when=FIRST_EXCEPTION)
        for future in done:
            future.result()

    def _run_transformer(
        self,
        transformer: BaseTransformer,
        data: bytes,
    ) -> None:
        output_data = transformer.transform(data)
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

        max_fanout = max(
            (len(ts) for ts in queue_to_transformers.values()),
            default=1,
        )
        self.message_executor = ThreadPoolExecutor(
            max_workers=self.max_concurrent_messages,
        )
        self.transformer_executor = ThreadPoolExecutor(
            max_workers=self.max_concurrent_messages * max_fanout,
        )
