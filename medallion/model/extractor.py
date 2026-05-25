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
from typing import Any, Callable, Iterator

from medallion.store.base import FOLDERNAME_DATETIME_FORMAT, BlobStore
from medallion.store.store import must_get_env

ARG_EXECUTION_START_TIME = "execution_start_time"
ARG_PREVIOUS_STEPS = "previous_steps"
ARG_IS_CHUNK_END = "is_chunk_end"
ORDERING_KEY_SEPARATOR = "|"
LOCAL_OUTPUT_DIR_ENV_VAR = "LOCAL_OUTPUT_DIR"
FORCE_RUN_EXTRACTOR_ENV_VAR = "FORCE_RUN_EXTRACTOR"


def is_force_extractor_run_enabled():
    force_extractor_run = must_get_env(FORCE_RUN_EXTRACTOR_ENV_VAR).lower() == "true"
    return force_extractor_run


def generate_utc_timestamp_foldername():
    start_time = pendulum.now("utc").format(FOLDERNAME_DATETIME_FORMAT)
    return start_time


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


class BaseExtractor[Out](
    ProcessingStep[Out],
    Writer[Out],
    ABC,
):
    @abstractmethod
    def extract(self) -> Iterator[Out]:
        pass

    def stream_output(
        self,
        data: Iterator[Any],
        stream_message_bytes: Callable[
            [
                bytes,
                dict[
                    str,
                    Any,
                ],
            ],
            None,
        ],
    ) -> None:
        start_time = generate_utc_timestamp_foldername()

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

        for next_item in data:
            output_buffer = self.write_output(current_item)
            if isinstance(output_buffer, list):
                for _buffer in output_buffer:
                    assert isinstance(_buffer, BytesIO)

                    output_data = _buffer.getvalue()
                    stream_message_bytes(output_data, args)
            else:
                output_data = output_buffer.getvalue()
                stream_message_bytes(output_data, args)

            current_item = next_item

        args[ARG_IS_CHUNK_END] = True
        final_output_buffer = self.write_output(current_item)

        if not isinstance(final_output_buffer, list):
            output_data = final_output_buffer.getvalue()
            stream_message_bytes(output_data, args)

            return

        for _buffer in final_output_buffer:
            output_data = _buffer.getvalue()
            stream_message_bytes(output_data, args)

    def load_or_extract_data(
        self,
        logger: Logger,
        store: BlobStore,
    ) -> Iterator[Any]:
        previous_run_filename: str | None = None

        force_run_extractor = is_force_extractor_run_enabled()
        if not force_run_extractor:
            logger.info(
                f"Checking for previous extractor output in folder[{self.name}]..."
            )
            previous_run_filename = store.find_latest_file_in_folder(
                self.name,
            )

        if not previous_run_filename or force_run_extractor:
            if force_run_extractor:
                logger.info(
                    "Force run extractor enabled, skipping cache check and running extractor"
                )
            else:
                logger.info(
                    f"No previous extractor output found in folder[{self.name}], running extractor"
                )

            return iter(self.extract())

        previous_run_file_path = f"{self.name}/{previous_run_filename}"
        data: list[Any] = []
        files_at_path = store.list_files_at(previous_run_file_path)
        logger.warning(
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
