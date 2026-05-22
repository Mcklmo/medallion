from io import BytesIO
from pydantic import BaseModel

from medallion.model.base import (
    BaseJSONStep,
    BasePydanticProcessingStep,
    ProcessingStep,
    Reader,
    Writer,
)

import json
from abc import ABC, abstractmethod
from typing import Iterator

from medallion.store.base import BlobStore
from medallion.store.store import must_get_env


class BaseTransformer[In, Out](
    ProcessingStep[Out],
    Writer[Out],
    Reader[In],
    ABC,
):
    @abstractmethod
    def transform(self, data: Iterator[In]) -> Iterator[Out]:
        pass

    def read_input_bytes(self, data: BytesIO) -> BytesIO:
        return data


class BaseStreamingTransformer[
    In,
    Out,
](
    ProcessingStep[Out],
    Writer[Out],
    Reader[In],
    ABC,
):
    @abstractmethod
    def transform_one(self, data: In) -> Out:
        pass

    def transform(self, data: Iterator[In]) -> Iterator[Out]:
        return [self.transform_one(item) for item in data]  # debug
        for item in data:
            yield self.transform_one(item)

    def read_input_bytes(self, data: BytesIO) -> BytesIO:
        return data


class BaseJSONTransformer[In, Out](BaseTransformer[In, Out], BaseJSONStep[Out], ABC):
    def read_bytes(self, data: BytesIO) -> Out:
        data.seek(0)
        return json.loads(data.read().decode())


class BasePydanticTransformer[
    In,
    Out: BaseModel,
](
    BaseTransformer[
        In,
        Out,
    ],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass


class BasePydanticStreamingTransformer[
    In,
    Out: BaseModel,
](
    BaseStreamingTransformer[
        In,
        Out,
    ],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass


class StoringTransformer[In](
    BaseTransformer[
        In,
        In,
    ],
):
    def __init__(
        self,
        store: BlobStore,
    ):
        self.store = store
        self.output_folder_name = must_get_env("OUTPUT_FOLDER_NAME")

    def transform(
        self,
        data: list[In],
    ) -> list[In]:
        self.store.upload_file(
            f"{self.output_folder_name}/data.json",
            BytesIO(json.dumps(data).encode()),
        )

        return []
