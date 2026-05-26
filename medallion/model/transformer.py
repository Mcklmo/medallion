from io import BytesIO
from logging import Logger
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
from medallion.store.base import must_get_env


class BaseTransformer[In, Out](
    ProcessingStep[Out],
    Writer[Out],
    Reader[In],
    ABC,
):
    def __init__(
        self,
        logger: Logger,
    ):
        self.logger = logger

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
    def __init__(
        self,
        logger: Logger,
    ):
        self.logger = logger

    @abstractmethod
    def transform_one(self, data: In) -> Out:
        pass

    def transform(self, data: Iterator[In]) -> Iterator[Out]:
        for item in data:
            parsed = self.transform_one(item)
            if isinstance(parsed, list):
                yield from parsed
                continue

            yield parsed

    def read_input_bytes(self, data: BytesIO | bytes) -> BytesIO:
        if isinstance(data, bytes):
            return BytesIO(data)

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
