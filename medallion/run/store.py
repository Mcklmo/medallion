import csv
from io import BytesIO, StringIO
import json
from medallion.log import create_logger
from medallion.queue.pubsub import PubSubQueue
from medallion.run.listener import Listener
from medallion.store.base import (
    BlobStore,
    build_timestamp_path_segments,
    must_get_env,
)
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

    def process_message(
        self,
        data: bytes,
        is_chunk_end: bool,
        start_time: str,
        previous_steps: list[str],
        item_index: int,
    ) -> None:
        destination_path_elements = (
            previous_steps + build_timestamp_path_segments(start_time) + [start_time]
        )
        destination_folder_path = "/".join(destination_path_elements)

        if not is_chunk_end:
            self.messages_hot_store.setdefault(destination_folder_path, []).append(data)
            return

        output_data = self.messages_hot_store.pop(destination_folder_path, []) + [data]
        for i, row in enumerate(output_data):
            file_prefix = f"{item_index}_{i}"

            try:
                json.loads(row)  # Check if it's valid JSON, if not treat as CSV
                self.store.upload_file(
                    destination_path=f"{destination_folder_path}/{file_prefix}.json",
                    content=BytesIO(row),
                )
            except json.JSONDecodeError:
                self.upload_csv_content(destination_folder_path, file_prefix, row)

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
    project_id = must_get_env("PUBSUB_PROJECT_ID")
    store = initialize_storage(
        must_get_env("LOCAL_OUTPUT_DIR"),
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
        max_retries=int(must_get_env("LISTENER_MAX_RETRIES")),
        logger=logger,
    )
    listener.listen()
