from io import BytesIO
from logging import Logger

from medallion.model.base import (
    BaseJSONStep,
    BasePydanticProcessingStep,
    ProcessingStep,
    Reader,
    Writer,
    DataModel,
)

import json
from abc import ABC, abstractmethod
from typing import Iterable

from medallion.store.base import BlobStore


class BaseTransformer[In: DataModel, Out: DataModel](
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

    def check_cache(
        self,
        store: BlobStore,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> str | None:
        raise NotImplementedError("check_cache is not implemented for BaseTransformer")

    def run(
        self,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> Iterable[Out]:
        assert previous_step_output is not None
        assert isinstance(previous_step_output, self.input_type)
        return self.transform(previous_step_output)


class BaseStreamingTransformer[
    In: DataModel,
    Out: DataModel,
](
    ProcessingStep[Out],
    Writer[Out],
    Reader[In],
    ABC,
):
    def __init__(
        self,
        logger: Logger,
        cache: BlobStore | None = None,
    ):
        self.logger = logger
        self._cache = cache

    @abstractmethod
    def transform_one(self, data: In) -> Out | list[Out]:
        pass

    def check_cache(
        self,
        store: BlobStore,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> str | None:
        if previous_step_output is None:
            raise ValueError(
                "previous_step_output is required for BaseStreamingTransformer"
            )

        if isinstance(previous_step_output, list):
            filename = DataModel.cache_list(
                self.name,
                previous_step_output,
            )
        else:
            filename = previous_step_output.default_cache_key(
                self.name,
            )

        if store.file_exists(filename):
            self.logger.info(f"Cache hit:  {filename}")
            return filename

        self.logger.info(f"Cache miss: {filename}")

        return None

    def run(
        self,
        previous_step_output: list[DataModel] | None = None,
    ) -> Iterable[Out] | Out:
        assert previous_step_output is not None

        for item in previous_step_output:
            assert isinstance(item, self.input_type)

            yield self.transform_one(item)


class BaseJSONTransformer[In: DataModel, Out: DataModel](
    BaseTransformer[In, Out], BaseJSONStep[Out], ABC
):
    def read_bytes(self, data: BytesIO) -> Out:
        data.seek(0)
        return json.loads(data.read().decode())


class BasePydanticTransformer[
    In: DataModel,
    Out: DataModel,
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
    In: DataModel,
    Out: DataModel,
](
    BaseStreamingTransformer[
        In,
        Out,
    ],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass
