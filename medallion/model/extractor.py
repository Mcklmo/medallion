from io import BytesIO
from logging import Logger

import pendulum
from pydantic import BaseModel, Field

from medallion.model.base import (
    BaseJSONStep,
    BasePydanticProcessingStep,
    ProcessingStep,
    Writer,
)


from abc import ABC, abstractmethod
from typing import Any, Iterator

from medallion.store.base import FOLDERNAME_DATETIME_FORMAT, BlobStore
from medallion.store.base import must_get_env

ARG_EXECUTION_START_TIME = "execution_start_time"
ARG_PREVIOUS_STEPS = "previous_steps"
ARG_IS_CHUNK_END = "is_chunk_end"
ARG_ITEM_INDEX = "item_index"
ORDERING_KEY_SEPARATOR = "|"
LOCAL_OUTPUT_DIR_ENV_VAR = "LOCAL_OUTPUT_DIR"
FORCE_RUN_EXTRACTOR_ENV_VAR = "FORCE_RUN_EXTRACTOR"


def is_force_extractor_run_enabled():
    return must_get_env(FORCE_RUN_EXTRACTOR_ENV_VAR).lower() == "true"


def generate_utc_timestamp_foldername(utc_timestamp: pendulum.DateTime) -> str:
    return utc_timestamp.format(FOLDERNAME_DATETIME_FORMAT)


class FileOutput(BaseModel):
    content: bytes
    is_full_file: bool = Field(
        alias="is_full_file",
        default=True,
    )


ARG_IS_FULL_FILE = FileOutput.model_fields["is_full_file"].alias
assert (
    ARG_IS_FULL_FILE == "is_full_file"
), "The alias for is_full_file must be 'is_full_file'"


class Streamer(ABC):
    @abstractmethod
    def stream(
        self,
        output_data: bytes,
        args: dict[
            str,
            Any,
        ],
    ) -> None:
        pass


class BaseExtractor[Out](
    ProcessingStep[Out],
    Writer[Out],
    ABC,
):
    def __init__(
        self,
        logger: Logger,
    ):
        self.logger = logger

    @abstractmethod
    def extract(self) -> Iterator[Out]:
        pass

    def stream_output(
        self,
        store: BlobStore,
        streamer: Streamer,
    ) -> None:
        utc_timestamp = pendulum.now("utc")
        start_time = generate_utc_timestamp_foldername(utc_timestamp)
        data = self.load_or_extract_data(
            store,
        )

        try:
            current_item = next(data)
        except StopIteration:
            return None

        args = {
            ARG_EXECUTION_START_TIME: start_time,
            ARG_PREVIOUS_STEPS: [
                self.name,
            ],
            ARG_IS_CHUNK_END: getattr(
                current_item,
                ARG_IS_FULL_FILE,  # if the file is complete, the listener processing the message expects ARG_IS_CHUNK_END to be true.
                False,
            ),
        }

        i = 0

        for next_item in data:
            args[ARG_ITEM_INDEX] = i
            self.send_stream_data(
                streamer,
                dict(args),
                current_item,
            )

            current_item = next_item
            i += 1

        args[ARG_ITEM_INDEX] = i
        args[ARG_IS_CHUNK_END] = True

        self.send_stream_data(
            streamer,
            dict(args),
            current_item,
        )

        self.logger.info(
            f"Extracting finished after {(pendulum.now('utc')-utc_timestamp).in_words()}"
        )

    def send_stream_data(
        self,
        streamer: Streamer,
        args: dict[str, Any],
        current_item: Out,
    ) -> None:
        output_buffer: BytesIO | list[BytesIO] = self.write_output(current_item)

        if not isinstance(output_buffer, list):
            output_data = output_buffer.getvalue()
            streamer.stream(
                output_data,
                args,
            )

            return

        for _buffer in output_buffer:
            output_data = _buffer.getvalue()
            streamer.stream(
                output_data,
                args,
            )

    def load_or_extract_data(
        self,
        store: BlobStore,
    ) -> Iterator[Out]:
        previous_run_filename: str | None = None

        force_run_extractor = is_force_extractor_run_enabled()
        if not force_run_extractor:
            self.logger.info(
                f"Checking for previous extractor output in folder[{self.name}]..."
            )
            previous_run_filename = store.find_latest_file_in_folder(
                self.name,
            )

        if not previous_run_filename or force_run_extractor:
            if force_run_extractor:
                self.logger.info(
                    "Force run extractor enabled, skipping cache check and running extractor"
                )
            else:
                self.logger.info(
                    f"No previous extractor output found in folder[{self.name}], running extractor"
                )

            return iter(self.extract())

        previous_run_file_path = f"{self.name}/{previous_run_filename}"
        data: list[Out] = []
        files_at_path = store.list_files_at(previous_run_file_path)
        self.logger.warning(
            f"Found previous extractor output: {previous_run_file_path}, loading {len(files_at_path)} files",
        )

        for filename in files_at_path:
            downloaded_file = store.download_file(filename)
            output = self.read_bytes(
                downloaded_file,
            )
            data.append(output)

        return iter(data)


class BaseJSONExtractor[Out](BaseExtractor[Out], BaseJSONStep[Out], ABC):
    pass


class BasePydanticExtractor[
    Out: BaseModel,
](
    BaseExtractor[Out],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass
