from io import BytesIO
from logging import Logger

import pendulum
from pydantic import BaseModel

from medallion.model.base import (
    BaseJSONStep,
    BasePydanticProcessingStep,
    ProcessingStep,
    Writer,
    FileOutput,
)


from abc import ABC, abstractmethod
from typing import Any, Iterable, Iterator, cast, Generator

from medallion.store.base import FOLDERNAME_DATETIME_FORMAT, BlobStore
from medallion.store.base import must_get_env

ARG_EXECUTION_START_TIME = "execution_start_time"
ARG_PREVIOUS_STEPS = "previous_steps"
ARG_IS_CHUNK_END = "is_chunk_end"
ARG_ITEM_INDEX = "item_index"
ORDERING_KEY_SEPARATOR = "|"
FORCE_RUN_EXTRACTOR_ENV_VAR = "FORCE_RUN_EXTRACTOR"


def is_force_extractor_run_enabled():
    return must_get_env(FORCE_RUN_EXTRACTOR_ENV_VAR).lower() == "true"


def generate_utc_timestamp_foldername(utc_timestamp: pendulum.DateTime) -> str:
    return utc_timestamp.format(FOLDERNAME_DATETIME_FORMAT)


ARG_IS_FULL_FILE: str = cast(str, FileOutput.model_fields["is_full_file"].alias)
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
        self.force_run_extractor = is_force_extractor_run_enabled()

    @abstractmethod
    def extract(self) -> Iterable[Out]:
        pass

    def stream_output(
        self,
        store: BlobStore,
        streamer: Streamer,
    ) -> None:
        utc_timestamp = pendulum.now("utc")
        start_time = generate_utc_timestamp_foldername(utc_timestamp)
        data = self.load_cache_or_run(
            store,
            self.logger,
            self.force_run_extractor,
            self.name,
            None,
        )

        if isinstance(data, self.output_type):
            data = [data]

        assert isinstance(data, Iterable)

        data = iter(data)

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

    def check_cache(
        self,
        store: BlobStore,
        previous_step_output: Any | None = None,
    ) -> str | None:
        return store.find_latest_file_in_folder(
            self.name,
        )

    def run(self, previous_step_output: Any | None = None) -> Iterable[Out]:
        return self.extract()


class BaseFileExtractor(
    BaseExtractor[FileOutput],
    ABC,
):
    def load_cached(self, data: BytesIO) -> FileOutput:
        extracted_bytes = data.getvalue()
        return FileOutput.model_validate_json(extracted_bytes)

    def write_output(
        self,
        output_data: Iterable[FileOutput] | FileOutput,
    ) -> BytesIO | list[BytesIO]:
        if isinstance(output_data, list) or isinstance(output_data, Generator):
            return [BytesIO(d.model_dump_json().encode()) for d in output_data]

        if not isinstance(output_data, FileOutput):
            raise ValueError(
                "output_data must be of type FileOutput or list[FileOutput]"
            )

        return BytesIO(output_data.model_dump_json().encode())


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
