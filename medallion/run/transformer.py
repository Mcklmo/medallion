import threading
from concurrent.futures import Future
from io import BytesIO
from typing import Any, Callable, cast
from medallion.log import create_logger
from medallion.model.base import DataModel
from medallion.model.extractor import (
    ARG_EXECUTION_START_TIME,
    ARG_IS_CHUNK_END,
    ARG_ITEM_INDEX,
    ARG_PARENT_ID,
    ARG_PART_COUNT,
    ARG_PART_INDEX,
    ARG_STORE_CACHE_AT_FOLDER,
)
from medallion.queue.pubsub import PubSubQueue
from medallion.resolve_classes import load_transformer_from_env
from medallion.model.transformer import (
    BaseGatherTransformer,
    BaseStreamingTransformer,
    BaseTransformer,
)
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR, Listener
from medallion.run.extractor import (
    ARG_PREVIOUS_STEPS,
    GOOGLE_CLOUD_PROJECT_ENV_VAR,
    ordering_key_from_steps,
)
from medallion.store.base import BlobStore, must_get_env, MEDALLION_TOPIC_ENV
from medallion.queue.base import Queue
from pydantic import BaseModel, Field, PrivateAttr

from medallion.store.initialize_storage import initialize_storage

FORCE_RUN_TRANSFORMER_ENV_VAR = "FORCE_RUN_TRANSFORMER"


def is_force_transformer_run_enabled():
    return must_get_env(FORCE_RUN_TRANSFORMER_ENV_VAR).lower() == "true"


class Args(BaseModel):
    execution_start_time: str = Field(alias=ARG_EXECUTION_START_TIME)
    previous_steps: list[str] = Field(alias=ARG_PREVIOUS_STEPS)
    is_chunk_end: bool = Field(alias=ARG_IS_CHUNK_END)
    item_index: int = Field(alias=ARG_ITEM_INDEX)
    store_cache_at_folder: str | None = Field(alias=ARG_STORE_CACHE_AT_FOLDER)
    parent_id: str | None = Field(default=None, alias=ARG_PARENT_ID)
    part_index: int | None = Field(default=None, alias=ARG_PART_INDEX)
    part_count: int | None = Field(default=None, alias=ARG_PART_COUNT)


def _flatten(output: Any) -> list[DataModel]:
    if isinstance(output, DataModel):
        return [output]

    return [item for o in output for item in _flatten(o)]


class _Batcher:
    """Runs the items of concurrently handled messages through one `transform_many` call."""

    def __init__(
        self,
        transform_many: Callable[[list[Any]], list[Any]],
        batch_size: int,
        max_wait: float,
    ) -> None:
        self._transform_many = transform_many
        self._batch_size = batch_size
        self._max_wait = max_wait
        self._lock = threading.Lock()
        self._pending: list[tuple[list[Any], Future]] = []

    def submit(self, items: list[Any]) -> list[Any]:
        """Blocks until the batch holding `items` has run; returns one result per item."""
        future: Future = Future()

        with self._lock:
            self._pending.append((items, future))
            is_full = sum(len(i) for i, _ in self._pending) >= self._batch_size
            batch = self._take() if is_full else []

        self._run(batch)

        try:
            return future.result(timeout=self._max_wait)
        except TimeoutError:
            pass

        # If another thread already took this item's batch, this runs whatever is pending instead
        # and then waits for that thread.
        with self._lock:
            batch = self._take()

        self._run(batch)

        return future.result()

    def _take(self) -> list[tuple[list[Any], Future]]:
        batch, self._pending = self._pending, []

        return batch

    def _run(self, batch: list[tuple[list[Any], Future]]) -> None:
        if not batch:
            return

        # ponytail: one bad item fails and retries the whole batch; retry items singly if that bites
        try:
            items = [item for items, _ in batch for item in items]
            results = self._transform_many(items)
            assert len(results) == len(
                items
            ), f"transform_many returned {len(results)} results for {len(items)} items"
        except Exception as e:
            for _, future in batch:
                future.set_exception(e)

            return

        start = 0
        for items, future in batch:
            future.set_result(results[start : start + len(items)])
            start += len(items)


class TransformerListener(Listener):
    transformer: BaseTransformer | BaseStreamingTransformer | BaseGatherTransformer
    messages_out: Queue
    messages_hot_store: list[bytes] = Field(
        init=False,
        default_factory=list,
        description="Store for messages that are currently being processed. Used for non-streaming transformers (because they require to see all messages before producing output).",
    )
    store: BlobStore
    force_run_transformer: bool
    _batcher: _Batcher | None = PrivateAttr(default=None)
    _gather_locks: dict[str, threading.Lock] = PrivateAttr(default_factory=dict)
    _gather_locks_guard: threading.Lock = PrivateAttr(default_factory=threading.Lock)

    def model_post_init(self, __context) -> None:
        transformer = self.transformer
        if (
            isinstance(transformer, BaseStreamingTransformer)
            and transformer.batch_size > 1
        ):
            # every message's thread blocks until its batch has run, so a smaller pool deadlocks
            self.max_concurrent_messages = max(
                self.max_concurrent_messages,
                transformer.batch_size,
            )
            self._batcher = _Batcher(
                transformer.transform_many,
                transformer.batch_size,
                transformer.max_batch_wait,
            )

        super().model_post_init(__context)

    def _after_listen(self) -> None:
        self.messages_out.close()

    def _write(self, output: BytesIO | list[BytesIO], args: Args) -> None:
        assert isinstance(output, BytesIO)

        self.messages_out.write(
            data=output.read(),
            args=args.model_dump(exclude_none=True),
            ordering_key=ordering_key_from_steps(args.previous_steps),
        )

    def process_message(
        self,
        data: list[bytes],
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
        store_cache_at_folder: str | None = None,
        message_args: dict[str, Any] | None = None,
    ) -> None:
        transformer = self.transformer

        if isinstance(transformer, BaseGatherTransformer):
            self._gather(
                transformer,
                cast(bytes, data),
                is_chunk_end,
                start_time,
                previous_steps,
                item_index,
                message_args or {},
            )

            return

        if isinstance(transformer, BaseStreamingTransformer):
            self._stream(
                transformer,
                data,
                is_chunk_end,
                start_time,
                previous_steps,
                item_index,
                message_args or {},
            )

            return

        self.messages_hot_store.append(data)

        if not is_chunk_end:
            return

        input_data = [transformer.read_input_bytes(d) for d in self.messages_hot_store]

        cache_path = DataModel.cache_list(
            self.transformer.name,
            input_data,
            self.transformer.version,
        )
        args = Args(
            execution_start_time=start_time,
            previous_steps=previous_steps + [transformer.name],
            is_chunk_end=is_chunk_end,
            item_index=item_index,
            store_cache_at_folder=cache_path,
        )

        output_data = transformer.load_cache_or_run(
            self.store,
            self.force_run_transformer,
            transformer.name,
            input_data,
        )
        self._write(transformer.write_output(output_data), args)
        self.messages_hot_store.clear()

    def _stream(
        self,
        transformer: BaseStreamingTransformer,
        data: list[bytes],
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
        message_args: dict[str, Any],
    ) -> None:
        input_data = list(transformer.read_input_bytes(data))
        cache_path = DataModel.cache_list(
            transformer.name,
            input_data,
            transformer.version,
        )
        is_cached = not self.force_run_transformer and bool(
            transformer.check_cache(self.store, input_data)
        )

        if is_cached or self._batcher is None:
            output_data = transformer.load_cache_or_run(
                self.store,
                self.force_run_transformer,
                transformer.name,
                input_data,
            )
        else:
            output_data = self._batcher.submit(input_data)

        items = _flatten(output_data)
        args = Args(
            execution_start_time=start_time,
            previous_steps=previous_steps + [transformer.name],
            is_chunk_end=is_chunk_end,
            item_index=item_index,
            store_cache_at_folder=cache_path,
            parent_id=message_args.get(ARG_PARENT_ID),
            part_index=message_args.get(ARG_PART_INDEX),
            part_count=message_args.get(ARG_PART_COUNT),
        )

        if not transformer.fan_out:
            self._write(transformer.write_output(items), args)

            return

        if not is_cached:
            # Written here in one piece, not per part by the StorageListener, which would store
            # the parts in arrival order and drop duplicates.
            self.store.upload_file(
                f"{cache_path}/data.json",
                cast(BytesIO, transformer.write_output(items)),
            )

        if not items:
            self.logger.warning(
                f"{transformer.name} fanned out into no parts; nothing reaches the gather step for item {item_index}"
            )

        parent_id = DataModel.hash(b"".join(i.stringify() for i in input_data))
        for i, item in enumerate(items):
            self._write(
                transformer.write_output(item),
                args.model_copy(
                    update={
                        "store_cache_at_folder": None,
                        "is_chunk_end": is_chunk_end and i == len(items) - 1,
                        "parent_id": parent_id,
                        "part_index": i,
                        "part_count": len(items),
                    }
                ),
            )

    def _gather_lock(self, key: str) -> threading.Lock:
        with self._gather_locks_guard:
            return self._gather_locks.setdefault(key, threading.Lock())

    def _gather(
        self,
        transformer: BaseGatherTransformer,
        data: bytes,
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
        message_args: dict[str, Any],
    ) -> None:
        parent_id = message_args.get(ARG_PARENT_ID)
        assert (
            parent_id is not None
        ), f"{transformer.name} needs parts from a transformer with fan_out = True, got args {message_args}"
        part_index: int = message_args[ARG_PART_INDEX]
        part_count: int = message_args[ARG_PART_COUNT]

        # Parts go to the blob store, not memory, so they survive restarts and the message can be
        # acked once its part is stored.
        prefix = f"gather/{transformer.name}/v{transformer.version}/{start_time}/{item_index}/{parent_id}"
        if is_chunk_end:
            # before the part, so whoever sees the last part also sees this
            self.store.upload_file(f"{prefix}/chunk_end", BytesIO(b""))

        self.store.upload_file(f"{prefix}/parts/{part_index:06d}.json", BytesIO(data))

        # ponytail: the lock is per process; two instances completing the same parent together can
        # both emit. The output is deterministic and cached; add a GCS generation-match claim if
        # duplicates matter.
        with self._gather_lock(prefix):
            if self.store.file_exists(f"{prefix}/done"):
                return

            part_files = sorted(self.store.list_files_at(f"{prefix}/parts"))
            if len(part_files) < part_count:
                return

            parts = [
                part
                for f in part_files
                for part in transformer.read_input_bytes(
                    self.store.download_file(f).getvalue()
                )
            ]
            output_data = transformer.load_cache_or_run(
                self.store,
                self.force_run_transformer,
                transformer.name,
                parts,
            )
            self._write(
                transformer.write_output(output_data),
                Args(
                    execution_start_time=start_time,
                    previous_steps=previous_steps + [transformer.name],
                    is_chunk_end=self.store.file_exists(f"{prefix}/chunk_end"),
                    item_index=item_index,
                    store_cache_at_folder=DataModel.cache_list(
                        transformer.name,
                        parts,
                        transformer.version,
                    ),
                ),
            )
            self.store.upload_file(f"{prefix}/done", BytesIO(b""))


if __name__ == "__main__":
    project_id = must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR)
    logger = create_logger()
    transformer = load_transformer_from_env(logger)
    store = initialize_storage(logger)
    listener = TransformerListener(
        transformer=transformer,
        messages_in=PubSubQueue(
            project_id=project_id,
            subscription_id=must_get_env("MEDALLION_SUBSCRIPTION"),
            logger=logger,
        ),
        messages_out=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env(MEDALLION_TOPIC_ENV),
            logger=logger,
        ),
        dlq=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env("MEDALLION_DLQ_TOPIC"),
            logger=logger,
        ),
        max_retries=int(must_get_env(LISTENER_MAX_RETRIES_ENV_VAR)),
        force_run_transformer=is_force_transformer_run_enabled(),
        logger=create_logger(),
        store=store,
    )
    listener.listen()
