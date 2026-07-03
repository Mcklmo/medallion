import threading
from concurrent.futures import Future, ThreadPoolExecutor
from logging import Logger
from typing import Any, Optional
from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr
from medallion.run.extractor import extract_and_stream
from medallion.run.store import StorageListener, PydanticFlatFileStore
from medallion.run.transformer import TransformerListener

from medallion.store.base import (
    BlobStore,
)
from medallion.queue.base import Queue
from medallion.model.store import BaseStore


class PipeLine(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    extractor: BaseExtractor
    transformers: Optional[list[BaseTransformer | BaseStreamingTransformer]]
    queues: list[Queue] = Field(
        default_factory=list,
        min_length=1,
    )
    logger: Logger
    store_output: BlobStore
    force_run_transformer: bool
    store: BaseStore | None = None
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

        def submit_listener(listen) -> None:
            future = executor.submit(listen)

            future.add_done_callback(self._on_listener_done)
            listener_futures.append(future)

        with ThreadPoolExecutor(max_workers=20) as executor:
            extractor_output_queue = self.queues[0]

            submit_listener(
                StorageListener(
                    messages_in=extractor_output_queue,
                    logger=self.logger,
                    store=PydanticFlatFileStore(
                        store=self.store_output,
                        logger=self.logger,
                        input_type=self.extractor.output_type,
                    ),
                    max_retries=0,
                    should_start_health_server=False,
                ).listen
            )

            last_queue = extractor_output_queue

            for i, t in enumerate(self.transformers or []):
                queue_in = self.queues[i]
                queue_out = self.queues[i + 1]
                last_queue = queue_out

                submit_listener(
                    TransformerListener(
                        messages_in=queue_in,
                        messages_out=queue_out,
                        logger=self.logger,
                        transformer=t,
                        max_retries=0,
                        should_start_health_server=False,
                        store=self.store_output,
                        force_run_transformer=self.force_run_transformer,
                    ).listen
                )

                submit_listener(
                    StorageListener(
                        messages_in=queue_out,
                        logger=self.logger,
                        store=PydanticFlatFileStore(
                            store=self.store_output,
                            logger=self.logger,
                            input_type=t.output_type,
                        ),
                        max_retries=0,
                        should_start_health_server=False,
                        cache_loader=t,
                    ).listen
                )

            if self.store is not None:
                submit_listener(
                    StorageListener(
                        messages_in=last_queue,
                        logger=self.logger,
                        store=self.store,
                        max_retries=0,
                        should_start_health_server=False,
                    ).listen
                )

            extraction_error: Exception | None = None
            try:
                extract_and_stream(
                    extractor=self.extractor,
                    queue_writer=extractor_output_queue,
                    store=self.store_output,
                )()
            except Exception as e:
                extraction_error = e
            finally:
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

    def model_post_init(self, context: Any) -> None:
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
                (BaseTransformer, BaseStreamingTransformer),
            ), f"Transformers must be of type {BaseTransformer.__name__} or {BaseStreamingTransformer.__name__}"

            assert t.input_type == previous_output_type, f"""\
                Transformer {t.__class__.__name__} expects input of type {t.input_type}, \
                but previous output is of type {previous_output_type}\
            """
            previous_output_type = t.output_type
