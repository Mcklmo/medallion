import csv
from io import BytesIO, StringIO
import json
from medallion.log import create_logger
from medallion.queue.pubsub import PubSubQueue
from medallion.run.extractor import GOOGLE_CLOUD_PROJECT_ENV_VAR
from medallion.run.listener import LISTENER_MAX_RETRIES_ENV_VAR, Listener
from medallion.store.base import (
    BlobStore,
    build_timestamp_path_segments,
    must_get_env,
)
from medallion.model.base import DataModel, Writer
from medallion.store.initialize_storage import initialize_storage
from pydantic import Field


class StorageListener(Listener):
    store: BlobStore
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
            f"Storing message for {previous_steps} at {start_time} with item index {item_index}"
        )
        destination_path_elements = (
            previous_steps + build_timestamp_path_segments(start_time) + [start_time]
        )
        destination_folder_path = "/".join(destination_path_elements)

        if not is_chunk_end:
            self.messages_hot_store.setdefault(destination_folder_path, []).append(data)
            return

        previous_step_output: DataModel | None = (
            self.cache_loader.load_cached(BytesIO(data)) if self.cache_loader else None
        )
        hash_path: str | None = None

        if previous_step_output is not None:
            assert isinstance(previous_step_output, DataModel)
            hash_path = previous_step_output.default_cache_key(previous_steps[-1])
        else:
            self.logger.warning(
                f"No previous step output found for {destination_folder_path}, skipping cache upload"
            )

        output_data = self.messages_hot_store.pop(destination_folder_path, []) + [data]
        for i, row in enumerate(output_data):
            file_prefix = f"{item_index}_{i}"

            try:
                json.loads(row)  # Check if it's valid JSON, if not treat as CSV
                self.store.upload_file(
                    destination_path=f"{destination_folder_path}/{file_prefix}.json",
                    content=BytesIO(row),
                )

                if hash_path is not None:
                    self.store.upload_file(
                        destination_path=f"{hash_path}/{file_prefix}.json",
                        content=BytesIO(row),
                    )
            except json.JSONDecodeError:
                self.upload_csv_content(destination_folder_path, file_prefix, row)

                if hash_path is not None:
                    self.upload_csv_content(hash_path, file_prefix, row)

    def upload_csv_content(
        self,
        destination_folder_path: str,
        file_prefix: str,
        potential_csv_files: bytes,
    ) -> None:
        reader = list(
            csv.reader(
                StringIO(potential_csv_files.decode()),
                delimiter=",",
            )
        )
        header = ",".join(reader[0])
        values = [",".join(r) for r in reader[1:]]
        csv_output_content = "\n".join(([header] if header else []) + values)

        self.store.upload_file(
            destination_path=f"{destination_folder_path}/{file_prefix}.csv",
            content=BytesIO(csv_output_content.encode()),
        )


if __name__ == "__main__":
    logger = create_logger()
    project_id = must_get_env(GOOGLE_CLOUD_PROJECT_ENV_VAR)
    store = initialize_storage(
        logger,
    )
    listener = StorageListener(
        store=store,
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
