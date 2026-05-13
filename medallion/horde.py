from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from typing import Any
from medallion.base import BaseExtractor, BaseTransformer
from medallion.stream import Message, MessageConsumer
from pydantic import BaseModel, ConfigDict, Field
import signal
import threading
import traceback
from logging import Logger


def generate_type_mismatch_message(
    previous_processor: BaseExtractor | BaseTransformer,
    next_processor: BaseTransformer,
) -> str:
    return f"Type mismatch for queue[{previous_processor.queue_to}]: \
        Previous step produces {previous_processor.output_type}, but next step {next_processor.name} consumes {next_processor.input_type}"


class Horde(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    transformers: list[BaseTransformer] = Field(default_factory=list)
    extractors: list[BaseExtractor]
    message_consumer: MessageConsumer
    logger: Logger
    max_concurrent_messages: int = 8
    queue_to_transformers: dict[str, list[BaseTransformer]] = Field(
        init=False, default_factory=dict
    )
    message_executor: ThreadPoolExecutor = Field(init=False, default=None)
    transformer_executor: ThreadPoolExecutor = Field(init=False, default=None)

    def run(self) -> None:
        shutdown = False

        def handle_signal(signum, frame):
            nonlocal shutdown
            shutdown = True
            self.logger.info("Shutdown requested")

        if threading.current_thread() is threading.main_thread():
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
        except Exception as e:
            self.logger.exception(f"Failed to process message: {e}")
            consumer.nack(message)

    def process_message(self, message: Message) -> None:
        queue_name = message.queue_name
        if not queue_name:
            extractor_name = message.args.get("extractor_name")
            assert (
                extractor_name
            ), "Received message without queue name or extractor name"

            # trigger extractor run to start a flow
            extractor = next(
                (e for e in self.extractors if e.__class__.__name__ == extractor_name),
                None,
            )
            assert extractor, f"Extractor {extractor_name} not found"

            self.logger.info(
                f"Received message for extractor[{extractor_name}], triggering extractor run"
            )

            try:
                output_data = extractor.extract()
            except Exception as e:
                self.logger.exception(
                    f"Extractor[{extractor_name}] failed: {e}\n{traceback.format_exc()}"
                )
                return

            output_bytes = extractor.write_output(output_data)
            self.message_consumer.publish(
                data=output_bytes.read(),
                queue_name=extractor.queue_to,
                args={},
            )

            return

        transformers = self.queue_to_transformers.get(queue_name, [])

        if not transformers:
            raise RuntimeError(
                f"Received message from queue[{queue_name}], but did not find any transformers"
            )

        self.logger.info(
            f"Received message from queue[{queue_name}], dispatching to {len(transformers)} transformers[{', '.join(t.name for t in transformers)}]"
        )

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
            args={},
        )

    def model_post_init(self, context: Any) -> None:
        queue_to_transformers: dict[str, list[BaseTransformer]] = {}

        for transformer in self.transformers:
            queue_to_transformers.setdefault(
                transformer.queue_from,
                [],
            ).append(transformer)

        # validate that types match for each queue
        self.validate_processor_types(
            self.extractors,
            queue_to_transformers,
        )
        self.validate_processor_types(
            self.transformers,
            queue_to_transformers,
        )

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

    def validate_processor_types(
        self,
        processors: list[BaseExtractor | BaseTransformer],
        queue_to_transformers: dict[str, list[BaseTransformer]],
    ) -> None:
        for extractor in processors:
            queue_transformers = queue_to_transformers.get(extractor.queue_to, [])
            for t in queue_transformers:
                assert (
                    t.input_type == extractor.output_type
                ), generate_type_mismatch_message(
                    extractor,
                    t,
                )
