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
from typing import Iterable


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
    def transform(self, data: Iterable[In]) -> Iterable[Out]:
        pass


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
    def transform_one(self, data: In) -> Out | list[Out]:
        pass

    def transform(self, data: Iterable[In]) -> Iterable[Out]:
        for item in data:
            parsed = self.transform_one(item)
            if isinstance(parsed, list):
                yield from parsed
                continue

            yield parsed


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
