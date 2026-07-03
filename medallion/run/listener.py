from medallion.model.extractor import (
    ARG_EXECUTION_START_TIME,
    ARG_IS_CHUNK_END,
    ARG_ITEM_INDEX,
    ARG_PREVIOUS_STEPS,
    ARG_STORE_CACHE_AT_FOLDER,
)
from medallion.queue.base import Message, Queue


from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


import faulthandler
import json
import os
import signal
import sys
import threading
from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from logging import Logger
from humanize import naturalsize


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({"ok": True}).encode())

    def log_message(self, format, *args):
        pass


def _start_health_server(logger) -> HTTPServer:
    port = int(os.getenv("PORT", "8080"))
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)

    thread.start()
    logger.info("Health-check server listening on port %d", port)

    return server


class Listener(
    BaseModel,
    ABC,
):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    messages_in: Queue
    max_retries: int
    logger: Logger
    dlq: Queue | None = None
    max_concurrent_messages: int = 8
    message_executor: ThreadPoolExecutor | None = Field(
        init=False,
        default=None,
    )
    shutdown: bool = Field(
        init=False,
        default=False,
    )
    should_start_health_server: bool = (
        True  # needed for GCP Cloud Run to probe the health of the container on startup
    )
    _fatal_error: BaseException | None = PrivateAttr(default=None)
    _fatal_error_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

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

        listener_name = self.__class__.__name__ + f"-{threading.current_thread().name}"
        self.logger.info(f"Starting listener[{listener_name}]")

        health_server = (
            None
            if not self.should_start_health_server
            else _start_health_server(self.logger)
        )

        assert self.message_executor is not None

        with self.messages_in as consumer:
            stream = consumer.read_stream()

            try:
                for message in stream:
                    if self.shutdown:
                        self.logger.info(f"{listener_name} Shutting down")
                        self._nack_safely(consumer, message)

                        break

                    self.logger.info(
                        f"{listener_name} Received message of size[{naturalsize(len(message.data))}] with args[{message.args}]"
                    )

                    future = self.message_executor.submit(
                        self._handle_message,
                        consumer,
                        message,
                    )
                    future.add_done_callback(self._on_message_done)
            except Exception as e:
                self.logger.exception(
                    f"Error in listener[{listener_name}]",
                    exc_info=e,
                )
            finally:
                self.logger.info(
                    f"{listener_name} shutting down, waiting for in-flight messages to complete..."
                )
                self.message_executor.shutdown(wait=True)

                # Close the stream generator explicitly so the queue can
                # release this consumer immediately instead of waiting for GC
                # (a traceback held by a fatal error keeps the frame alive).
                close_stream = getattr(stream, "close", None)
                if close_stream is not None:
                    close_stream()

                self._after_listen()

                if self.should_start_health_server:
                    if health_server is None:
                        raise Exception("Health server is unexpectedly None")

                    health_server.shutdown()

        if self._fatal_error is not None:
            raise self._fatal_error

    def _on_message_done(self, future: Future) -> None:
        exception = future.exception()
        if exception is None:
            return

        try:
            self.logger.exception(
                "Fatal error in message handler, shutting down",
                exc_info=exception,
            )
        finally:
            with self._fatal_error_lock:
                if self._fatal_error is None:
                    self._fatal_error = exception

            self.request_shutdown()

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
            item_index = message.args.get(ARG_ITEM_INDEX)

            if item_index is None:
                raise ValueError(
                    f"Message args missing required {ARG_ITEM_INDEX}: {message.args}"
                )

            assert (
                start_time and previous_steps
            ), f"message[{message.args}] incomplete. Missing required message args: {ARG_EXECUTION_START_TIME} or {ARG_PREVIOUS_STEPS}"

            self.process_message(
                message.data,
                is_chunk_end,
                start_time,
                previous_steps,
                item_index,
            )
            queue.ack(message)
        except Exception as e:
            assert message.delivery_attempt is not None, (
                "Message.delivery_attempt is None — the subscription is missing a "
                "dead_letter_policy. Re-run bootstrap to provision it."
            )

            if message.delivery_attempt >= self.max_retries:
                if not self.dlq:
                    # Release the message before halting so in-memory queues
                    # can drain and the broker can redeliver after restart.
                    self._nack_safely(queue, message)

                    raise e

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

            self._nack_safely(queue, message)

    def _nack_safely(
        self,
        queue: Queue,
        message: Message,
    ) -> None:
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
        data: list[bytes],
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
    ) -> None:
        pass

    def model_post_init(self, __context) -> None:
        self.message_executor = ThreadPoolExecutor(
            max_workers=self.max_concurrent_messages,
        )


LISTENER_MAX_RETRIES_ENV_VAR = "LISTENER_MAX_RETRIES"
