from io import BytesIO
import json
import threading
from medallion.log import create_logger
from medallion.queue.pubsub import PubSubQueue
from medallion.model.store import BaseStore
from medallion.run.extractor import GOOGLE_CLOUD_PROJECT_ENV_VAR
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR, Listener
from medallion.store.base import (
    BlobStore,
    build_timestamp_path_segments,
    must_get_env,
)
from medallion.model.base import DataModel, PydanticReader, Writer
from medallion.store.initialize_storage import initialize_storage
from pydantic import Field

from logging import Logger


class PydanticFlatFileStore[In: DataModel](
    BaseStore[In],
    PydanticReader[In],
):
    def __init__(
        self,
        store: BlobStore,
        logger: Logger,
        input_type: type[In] | None = None,
    ) -> None:
        super().__init__(logger)
        self.store = store
        self.input_type = input_type  # shadows the classproperty on this instance; used by read_input_bytes
        self._path_locks: dict[str, threading.Lock] = {}
        self._path_locks_guard = threading.Lock()

    def _lock_for(self, path: str) -> threading.Lock:
        with self._path_locks_guard:
            lock = self._path_locks.get(path)
            if lock is None:
                lock = threading.Lock()
                self._path_locks[path] = lock

            return lock

    def store_message_data(
        self,
        file_prefix: str,
        row: list[In],
        destination_folder_path: str,
        store_cache_at_folder: str | None = None,
    ) -> None:
        destination_path = f"{destination_folder_path}/{file_prefix}.json"

        with self._lock_for(destination_path):
            content = self.load_and_combine_data(
                row,
                destination_path,
            )

            content_items: list[dict] = [
                item.model_dump(
                    by_alias=True,
                )
                for item in content
            ]

            json_bytes_io = BytesIO(json.dumps(content_items, indent=2).encode())
            self.store.upload_file(
                destination_path=f"{destination_folder_path}/{file_prefix}.json",
                content=json_bytes_io,
            )

            if store_cache_at_folder is None:
                self.logger.warning(
                    f"store_cache_at_folder is None. Skipping cache storage for {destination_path}"
                )
                return

            self.store.upload_file(
                destination_path=f"{store_cache_at_folder}/{file_prefix}.json",
                content=json_bytes_io,
            )

    def load_and_combine_data(
        self,
        row: list[In],
        destination_path: str,
    ) -> list[In]:
        content: dict[In, None] = {}

        if self.store.file_exists(destination_path):
            _content_bytes = self.store.download_file(destination_path)
            for item in self.read_input_bytes(_content_bytes.getvalue()):
                content[item] = None

        for item in row:
            content[item] = None

        return list(content.keys())


class StorageListener(Listener):
    store: BaseStore
    messages_hot_store: dict[
        str,
        list[bytes],
    ] = Field(
        init=False,
        default_factory=dict,
        description="Store for messages that are currently being processed. Keys are unique per pipeline.",
    )
    cache_loader: Writer | None = None

    def process_message(
        self,
        data: list[bytes] | bytes,
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
        store_cache_at_folder: str | None = None,
    ) -> None:

        self.logger.info(
            f"Storing message for {previous_steps} at {start_time} with item index {item_index} (thread: {threading.current_thread().name})"
        )

        destination_path_elements = (
            previous_steps + build_timestamp_path_segments(start_time) + [start_time]
        )
        destination_folder_path = "/".join(destination_path_elements)

        _data = self.store.read_input_bytes(data)

        self.store.store_message_data(
            "data",
            _data,
            destination_folder_path,
            store_cache_at_folder,
        )


if __name__ == "__main__":
    logger = create_logger()
    project_id = must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR)
    store = initialize_storage(
        logger,
    )
    listener = StorageListener(
        store=PydanticFlatFileStore(
            store=store,
            logger=logger,
        ),
        messages_in=PubSubQueue(
            project_id=project_id,
            subscription_id=must_get_env("MEDALLION_SUBSCRIPTION"),
            logger=logger,
        ),
        dlq=PubSubQueue(
            project_id=project_id,
            topic_id=must_get_env("MEDALLION_DLQ_TOPIC"),
            logger=logger,
        ),
        max_retries=int(must_get_env(LISTENER_MAX_RETRIES_ENV_VAR)),
        logger=logger,
    )
    listener.listen()
