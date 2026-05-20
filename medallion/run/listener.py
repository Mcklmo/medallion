from medallion.run.extractor import ARG_EXECUTION_START_TIME, ARG_PREVIOUS_STEPS
from medallion.stream import Message, Queue


from pydantic import BaseModel, ConfigDict, Field


import faulthandler
import signal
import sys
import threading
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from logging import Logger


class Listener(
    BaseModel,
    ABC,
):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    messages_in: Queue
    logger: Logger
    max_concurrent_messages: int = 8
    message_executor: ThreadPoolExecutor = Field(
        init=False,
        default=ThreadPoolExecutor(
            max_workers=max_concurrent_messages,
        ),
    )

    def run(self) -> None:
        shutdown = False

        def handle_signal(signum, frame):
            nonlocal shutdown
            shutdown = True
            self.logger.info("Shutdown requested")

        def handle_dump_signal(signum, frame):
            self.logger.info("Dumping all thread stacks")
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)

        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, handle_signal)
            signal.signal(signal.SIGTERM, handle_signal)
            if hasattr(signal, "SIGUSR1"):
                signal.signal(signal.SIGUSR1, handle_dump_signal)

        with self.messages_in as consumer:
            try:
                for message in consumer.messages():
                    if shutdown:
                        self.logger.info("Shutting down listener")
                        break

                    self.logger.info(f"Received message: {message}")

                    self.message_executor.submit(
                        self._handle_message,
                        consumer,
                        message,
                    )
            finally:
                self.message_executor.shutdown(wait=True)

    def _handle_message(
        self,
        queue: Queue,
        message: Message,
    ) -> None:
        try:
            is_chunk_end = message.args.get("is_chunk_end", False)
            start_time = message.args.get(ARG_EXECUTION_START_TIME)
            previous_steps = message.args.get(ARG_PREVIOUS_STEPS)

            assert (
                start_time and previous_steps
            ), f"message[{message}] incomplete. Missing required message args: {ARG_EXECUTION_START_TIME} or {ARG_PREVIOUS_STEPS}"

            self.process_message(
                message.data,
                is_chunk_end,
                start_time,
                previous_steps,
            )
            queue.ack(message)
        except Exception:
            self.logger.exception(f"Error processing message: {message}")
            try:
                queue.nack(message)
                self.logger.info(f"nacked message: {message}")
            except Exception:
                self.logger.exception(f"Failed to nack message: {message}")
            raise

    @abstractmethod
    def process_message(
        self,
        data: bytes,
        is_chunk_end: bool,
        start_time: float,
        previous_steps: list[str],
    ) -> None:
        pass
