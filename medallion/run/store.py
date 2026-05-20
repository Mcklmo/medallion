from io import BytesIO
import json
from medallion.log import create_logger
from medallion.queue.pubsub import PubSubQueue
from medallion.run.listener import Listener
from medallion.store.base import BlobStore
from medallion.store.store import initialize_storage, must_get_env
from pydantic import Field


class StorageListener(Listener):
    store: BlobStore
    output_file_extension: str
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
    ) -> None:
        destination_path = "/".join(previous_steps + [start_time] + ["data.csv"])

        if not is_chunk_end:
            self.messages_hot_store.setdefault(destination_path, []).append(data)
            return

        output_data = self.messages_hot_store.pop(destination_path, []) + [data]

        header: str | None = None
        for row in output_data:
            parsed_json = json.loads(row)
            parsed_json = {key: value for key, value in sorted(parsed_json.items())}
            _header = ",".join(list(parsed_json.keys()))

            if header is None:
                header = _header

            assert header == _header, (
                "Inconsistent header in output data: " + header + " vs " + _header
            )

        values: list[str] = []
        for row in output_data:
            json_data = json.loads(row)
            json_data = {key: value for key, value in sorted(json_data.items())}
            value = ",".join([str(v) for v in json_data.values()])

            values.append(value)

        self.store.upload_file(
            destination_path=destination_path,
            content=BytesIO("\n".join([header] + values).encode()),
        )


if __name__ == "__main__":
    logger = create_logger()
    store = initialize_storage(
        must_get_env("LOCAL_OUTPUT_DIR"),
        logger,
    )
    listener = StorageListener(
        store=store,
        messages_in=PubSubQueue(
            project_id=must_get_env("PUBSUB_PROJECT_ID"),
            subscription_id=must_get_env("MEDALLION_SUBSCRIPTION"),
            logger=logger,
        ),
        logger=logger,
        output_file_extension="jsonl",
    )
    listener.run()
