from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Any, Optional
from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from pydantic import BaseModel, ConfigDict, Field
from medallion.queue.mock import MockQueue
from medallion.run.extractor import extract_and_stream
from medallion.run.store import StorageListener
from medallion.run.transformer import TransformerListener
from medallion.store.base import (
    BlobStore,
)
from medallion.queue.base import Queue


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
    dlqs: list[Queue] = Field(init=False, default_factory=list)
    logger: Logger
    store_output: BlobStore
    store_cache: BlobStore

    def run(self) -> None:
        with ThreadPoolExecutor(max_workers=20) as executor:
            extractor_output_queue = self.queues[0]

            executor.submit(
                StorageListener(
                    messages_in=extractor_output_queue,
                    dlq=self.dlqs[0],
                    logger=self.logger,
                    store=self.store_output,
                    max_retries=0,
                    should_start_health_server=False,
                ).listen
            )

            for i, t in enumerate(self.transformers or []):
                queue_in = self.queues[i]
                queue_out = self.queues[i + 1]
                dlq = self.dlqs[i + 1]

                executor.submit(
                    TransformerListener(
                        messages_in=queue_in,
                        messages_out=queue_out,
                        dlq=dlq,
                        logger=self.logger,
                        transformer=t,
                        max_retries=0,
                        should_start_health_server=False,
                    ).listen
                )
                executor.submit(
                    StorageListener(
                        messages_in=queue_out,
                        dlq=dlq,
                        logger=self.logger,
                        store=self.store_output,
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
        assert (
            len(self.queues) == len(self.transformers or []) + 1
        ), "Number of queues must be equal to number of transformers + 1 (for extractor output)"

        previous_output_type = self.extractor.output_type

        # one dlq per queue
        for _ in self.queues:
            self.dlqs.append(MockQueue(messages=[]))

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
