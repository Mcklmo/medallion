from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any, Optional
from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from pydantic import BaseModel, ConfigDict, Field
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

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=20) as executor:
            extractor_output_queue = self.queues[0]

            executor.submit(
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

                executor.submit(
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

                executor.submit(
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
                executor.submit(
                    StorageListener(
                        messages_in=last_queue,
                        logger=self.logger,
                        store=self.store,
                        max_retries=0,
                        should_start_health_server=False,
                    ).listen
                )

            try:
                extract_and_stream(
                    extractor=self.extractor,
                    queue_writer=extractor_output_queue,
                    store=self.store_output,
                )()
            finally:
                # Close every queue so all listener loops terminate, even if
                # extraction failed partway through. Without this, run()'s
                # ThreadPoolExecutor.__exit__ -> shutdown(wait=True) would
                # block forever on listeners stuck reading from open queues,
                # and the real exception would never propagate out of run().
                for q in self.queues:
                    q.close()

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
