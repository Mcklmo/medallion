from io import BytesIO
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

    def store_message_data(
        self,
        file_prefix: str,
        row: In,
        destination_folder_path: str,
        cache_path: str | None,
    ) -> None:
        data = row.model_dump_json(
            by_alias=True,
            indent=2,
        ).encode()
        self.store.upload_file(
            destination_path=f"{destination_folder_path}/{file_prefix}.json",
            content=BytesIO(data),
        )

        if cache_path is None:
            return

        self.store.upload_file(
            destination_path=f"{cache_path}/{file_prefix}.json",
            content=BytesIO(data),
        )


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
        data: bytes,
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
    ) -> None:
        self.logger.info(
            f"Storing message for {previous_steps} at {start_time} with item index {item_index} (thread: {threading.current_thread().name})"
        )

        destination_path_elements = (
            previous_steps + build_timestamp_path_segments(start_time) + [start_time]
        )
        destination_folder_path = "/".join(destination_path_elements)

        if not is_chunk_end:
            self.messages_hot_store.setdefault(destination_folder_path, []).append(data)
            return

        cache_path = self.generate_cache_path(
            data,
            previous_steps[-1],
            destination_folder_path,
        )

        output_data = self.messages_hot_store.pop(destination_folder_path, []) + [data]
        for i, row in enumerate(output_data):
            _data = self.store.read_input_bytes(row)
            self.store.store_message_data(
                f"{item_index}_{i}",
                _data,
                destination_folder_path,
                cache_path,
            )

    def generate_cache_path(
        self,
        data: bytes,
        previous_step: str,
        destination_folder_path: str,
    ) -> str | None:
        previous_step_output: DataModel | None = (
            self.cache_loader.load_cached(BytesIO(data)) if self.cache_loader else None
        )

        if previous_step_output is None:
            self.logger.warning(
                f"No previous step output found for {destination_folder_path}, skipping cache upload"
            )

            return None

        assert isinstance(previous_step_output, DataModel)

        return previous_step_output.default_cache_key(previous_step)


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
