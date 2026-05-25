from medallion.model.extractor import (
    ARG_EXECUTION_START_TIME,
    ARG_IS_CHUNK_END,
    ARG_PREVIOUS_STEPS,
)
from medallion.stream import Message, Queue


from pydantic import BaseModel, ConfigDict, Field


import faulthandler
import signal
import sys
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from humanize import naturalsize


class Listener(
    BaseModel,
    ABC,
):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    messages_in: Queue
    dlq: Queue
    max_retries: int
    logger: Logger
    max_concurrent_messages: int = 8
    message_executor: ThreadPoolExecutor = Field(
        init=False,
        default=ThreadPoolExecutor(
            max_workers=max_concurrent_messages,
        ),
    )
    shutdown: bool = Field(
        init=False,
        default=False,
    )

    def request_shutdown(self) -> None:
        self.shutdown = True
        self.logger.info("Shutdown requested")
        self.messages_in.close()

    def listen(self) -> None:
        def handle_signal(signum, frame):
            self.shutdown = True
            self.logger.info("Shutdown requested")
            self.messages_in.close()

        def handle_dump_signal(signum, frame):
            self.logger.info("Dumping all thread stacks")
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
            if hasattr(signal, "SIGUSR1"):
                signal.signal(signal.SIGUSR1, handle_dump_signal)

        self.logger.info(f"Starting listener[{self.__class__.__name__}]")

        with self.messages_in as consumer:
            try:
                for message in consumer.read_stream():
                    if self.shutdown:
                        self.logger.info("Shutting down listener")
                        break

                    self.logger.info(
                        f"Received message of size[{naturalsize(len(message.data))}] with args[{message.args}]"
                    )

                    self.message_executor.submit(
                        self._handle_message,
                        consumer,
                        message,
                    )
            finally:
                self.message_executor.shutdown(wait=True)
                self._after_listen()

    def _after_listen(self) -> None:
        """Hook for subclasses; runs once the listener has fully drained."""

    def _handle_message(
        self,
        queue: Queue,
        message: Message,
    ) -> None:
        try:
            is_chunk_end = message.args.get(ARG_IS_CHUNK_END, False)
            start_time = message.args.get(ARG_EXECUTION_START_TIME)
            previous_steps = message.args.get(ARG_PREVIOUS_STEPS)

            assert (
                start_time and previous_steps
            ), f"message[{message.args}] incomplete. Missing required message args: {ARG_EXECUTION_START_TIME} or {ARG_PREVIOUS_STEPS}"

            self.process_message(
                message.data,
                is_chunk_end,
                start_time,
                previous_steps,
            )
            queue.ack(message)
        except Exception as e:
            assert message.delivery_attempt is not None, (
                "Message.delivery_attempt is None — the subscription is missing a "
                "dead_letter_policy. Re-run bootstrap to provision it."
            )

            if message.delivery_attempt >= self.max_retries:
                self.logger.exception(
                    f"Dead-lettering message after {message.delivery_attempt} attempts: {message.args}",
                    exc_info=e,
                )
                self.dlq.write(
                    data=message.data,
                    args={
                        **message.args,
                        "_failure_reason": str(e),
                        "_attempts": message.delivery_attempt,
                    },
                    ordering_key=str(message.delivery_attempt),
                )
                queue.ack(message)
                return

            self.logger.exception(
                f"Sending nack for error processing message (attempt {message.delivery_attempt}/{self.max_retries}): {message.args}",
                exc_info=e,
            )

            try:
                queue.nack(message)
            except Exception as e:
                self.logger.exception(
                    f"Failed to nack message: {message.args}",
                    exc_info=e,
                )

    @abstractmethod
    def process_message(
        self,
        data: bytes,
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
    ) -> None:
        pass
