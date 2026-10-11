import signal
import threading
from contextlib import contextmanager
from concurrent.futures import Future, ThreadPoolExecutor
from logging import Logger
from typing import Any, Optional
from medallion.log import set_thread_processor
from medallion.model.transformer import BaseTransformer
from medallion.progress import Progress, show_progress
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import (
    TRANSFORMER_TYPES,
    BaseGatherTransformer,
    BaseStreamingTransformer,
)
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from medallion.run.extractor import extract_and_stream
from medallion.run.listener import Listener
from medallion.run.store import StorageListener, PydanticFlatFileStore
from medallion.run.transformer import TransformerListener

from medallion.store.base import (
    BlobStore,
)
from medallion.queue.base import Queue
from medallion.model.store import BaseStore


@contextmanager
def _stop_listeners_on_signal(listeners: list[Listener]):
    # listeners run off the main thread here, so their own signal handlers are never installed
    if threading.current_thread() is not threading.main_thread():
        yield

        return

    def stop(signum, frame):
        for listener in listeners:
            listener.request_shutdown()

        raise KeyboardInterrupt

    previous = {s: signal.signal(s, stop) for s in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


class PipeLine(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    extractor: BaseExtractor
    transformers: Optional[
        list[BaseTransformer | BaseStreamingTransformer | BaseGatherTransformer]
    ]
    queues: list[Queue] = Field(
        default_factory=list,
        min_length=1,
    )
    logger: Logger
    store_output: BlobStore
    force_run_transformer: bool
    store: BaseStore | None = None
    # in run order: extractor, transformers, store; defaults to their class names
    processor_names: list[str] = Field(default_factory=list)
    _listener_error: BaseException | None = PrivateAttr(default=None)
    _listener_error_lock: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def _on_listener_done(self, future: Future) -> None:
        exception = future.exception()
        if exception is None:
            return

        with self._listener_error_lock:
            if self._listener_error is None:
                self._listener_error = exception

        # A listener died; close every queue so extraction and the remaining
        # listeners stop instead of feeding a dead pipeline stage.
        for q in self.queues:
            q.close()

    def run(self) -> None:
        listener_futures: list[Future] = []
        listeners: list[Listener] = []

        def submit_listener(listener: Listener) -> None:
            listeners.append(listener)
            future = executor.submit(listener.listen)

            future.add_done_callback(self._on_listener_done)
            listener_futures.append(future)

        with (
            show_progress(self.processor_names, self.logger) as progress,
            ThreadPoolExecutor(max_workers=20) as executor,
            _stop_listeners_on_signal(listeners),
        ):
            extractor_output_queue = self.queues[0]
            extractor_name = self.processor_names[0]

            # an output's store listener logs as the processor that produced it
            submit_listener(
                StorageListener(
                    messages_in=extractor_output_queue,
                    processor=extractor_name,
                    logger=self.logger,
                    store=PydanticFlatFileStore(
                        store=self.store_output,
                        logger=self.logger,
                        input_type=self.extractor.output_type,
                    ),
                    max_retries=0,
                    should_start_health_server=False,
                )
            )

            last_queue = extractor_output_queue

            for i, t in enumerate(self.transformers or []):
                queue_in = self.queues[i]
                queue_out = self.queues[i + 1]
                last_queue = queue_out
                transformer_name = self.processor_names[i + 1]

                submit_listener(
                    TransformerListener(
                        messages_in=queue_in,
                        messages_out=queue_out,
                        processor=transformer_name,
                        progress=progress[i + 1],
                        logger=self.logger,
                        transformer=t,
                        max_retries=0,
                        should_start_health_server=False,
                        store=self.store_output,
                        force_run_transformer=self.force_run_transformer,
                    )
                )

                submit_listener(
                    StorageListener(
                        messages_in=queue_out,
                        processor=transformer_name,
                        logger=self.logger,
                        store=PydanticFlatFileStore(
                            store=self.store_output,
                            logger=self.logger,
                            input_type=t.output_type,
                        ),
                        max_retries=0,
                        should_start_health_server=False,
                        cache_loader=t,
                    )
                )

            if self.store is not None:
                submit_listener(
                    StorageListener(
                        messages_in=last_queue,
                        processor=self.processor_names[-1],
                        progress=progress[-1],
                        logger=self.logger,
                        store=self.store,
                        max_retries=0,
                        should_start_health_server=False,
                    )
                )

            extraction_error: Exception | None = None
            set_thread_processor(extractor_name)
            try:
                extract_and_stream(
                    extractor=self.extractor,
                    queue_writer=extractor_output_queue,
                    store=self.store_output,
                    progress=progress[0],
                )()
                self._set_totals(progress)
            except Exception as e:
                extraction_error = e
            finally:
                set_thread_processor(None)
                # Close every queue so all listener loops terminate, even if
                # extraction failed partway through. Without this, run()'s
                # ThreadPoolExecutor.__exit__ -> shutdown(wait=True) would
                # block forever on listeners stuck reading from open queues.
                # Draining in pipeline order guarantees each stage has
                # forwarded everything downstream before the next queue closes.
                for q in self.queues:
                    q.close()
                    q.wait_drained()

        # The executor has fully drained here. A listener failure is the root
        # cause — extraction errors are often just a consequence of the
        # queues being closed on it — so it takes precedence.
        if self._listener_error is not None:
            raise self._listener_error

        if extraction_error is not None:
            raise extraction_error

    def _set_totals(self, progress: list[Progress | None]) -> None:
        """Gives each processor the extracted count as its total, up to and including the first
        transformer whose messages out don't match its messages in."""
        if progress[0] is None:
            return

        total = progress[0].handled
        transformers = self.transformers or []

        for i, p in enumerate(progress):
            assert p is not None
            p.set_total(total)

            # progress[i] after the extractor is transformers[i - 1], or the store
            t = transformers[i - 1] if 0 < i <= len(transformers) else None
            if t is not None and not (isinstance(t, BaseStreamingTransformer) and not t.fan_out):
                return

    def model_post_init(self, context: Any) -> None:
        processors = [self.extractor, *(self.transformers or [])]
        if self.store is not None:
            processors.append(self.store)

        if not self.processor_names:
            self.processor_names = [type(p).__name__ for p in processors]

        assert len(self.processor_names) == len(
            processors
        ), f"Expected {len(processors)} processor names, got {self.processor_names}"

        transformer_count = len(self.transformers or [])
        storage_queue_count = 1 if self.store is not None else 0
        expected_queue_count = transformer_count + 1 + storage_queue_count
        assert (
            len(self.queues) == expected_queue_count
        ), f"Expected {expected_queue_count} queues (number of transformers[{transformer_count}] + storage_queue_count[{storage_queue_count}] + 1 for extractor output). Got {len(self.queues)} queues."

        previous_output_type = self.extractor.output_type

        for t in self.transformers or []:
            assert isinstance(
                t,
                TRANSFORMER_TYPES,
            ), f"Transformers must be one of {TRANSFORMER_TYPES}"

            assert t.input_type == previous_output_type, f"""\
                Transformer {t.__class__.__name__} expects input of type {t.input_type}, \
                but previous output is of type {previous_output_type}\
            """
            previous_output_type = t.output_type
