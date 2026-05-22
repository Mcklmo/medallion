import csv
from io import BytesIO, StringIO
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
        destination_folder_path = "/".join(previous_steps + [start_time])

        if not is_chunk_end:
            self.messages_hot_store.setdefault(destination_folder_path, []).append(data)
            return

        output_data = self.messages_hot_store.pop(destination_folder_path, []) + [data]
        values: list[str] = []
        header: str = ""
        found_csv_file = False

        for i, row in enumerate(output_data):
            try:
                parsed_json = json.loads(row)
            except json.decoder.JSONDecodeError as e:
                try:
                    self.upload_csv_content(destination_folder_path, header, i, row)
                    found_csv_file = True

                    continue

                except Exception as e:
                    self.logger.error(
                        f"Failed to parse row as JSON or CSV for {destination_folder_path}: {row[:1000]}...",
                        exc_info=e,
                    )
                    continue

            parsed_json = {key: value for key, value in sorted(parsed_json.items())}
            _header = ",".join(list(parsed_json.keys()))

            if not header:
                header = _header

            assert (
                header == _header
            ), f"Inconsistent header in output data: [{header}] vs [{_header}]"

            value = ",".join([str(v) for v in parsed_json.values()])
            values.append(value)

        try:
            csv_output_content = "\n".join([header] if header else [] + values)
        except TypeError as e:
            self.logger.error(
                f"Failed to generate CSV content for path[{destination_folder_path}] with header[{header}] and values[{values}]",
                exc_info=e,
            )
            return

        if not csv_output_content:
            if found_csv_file:
                return

            self.logger.warning(
                f"No valid CSV content generated for {destination_folder_path}, skipping upload."
            )
            return

        self.store.upload_file(
            destination_path=f"{destination_folder_path}/data.csv",
            content=BytesIO(csv_output_content.encode()),
        )

    def upload_csv_content(
        self,
        destination_folder_path: str,
        header: str,
        i: int,
        potential_csv_files: bytes,
    ) -> None:
        reader = list(
            csv.reader(
                StringIO(potential_csv_files.decode()),
                delimiter=",",
            )
        )
        _header = ",".join(reader[0])
        if not header:
            header = _header

        assert (
            header == _header
        ), f"Inconsistent header in output data: [{header}] vs [{_header}]"

        values = [",".join(r) for r in reader[1:]]
        csv_output_content = "\n".join(([header] if header else []) + values)

        self.store.upload_file(
            destination_path=f"{destination_folder_path}/{i}.csv",
            content=BytesIO(csv_output_content.encode()),
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
    listener.listen()
