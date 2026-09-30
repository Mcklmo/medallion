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
from typing import ClassVar, Iterable

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
    batch_size: ClassVar[int] = 1
    """Messages the listener collects before calling `transform_many`."""
    max_batch_wait: ClassVar[float] = 0.5
    """Seconds the listener waits for a batch to fill before running it anyway."""
    fan_out: ClassVar[bool] = False
    """Publish each item of a list output as its own message, for a `BaseGatherTransformer` to collect."""

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

    def transform_many(self, items: list[In]) -> list[Out | list[Out]]:
        """Override to process a batch at once; returns one result per item, in order."""
        return [self.transform_one(item) for item in items]

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
                self.version,
            )
        else:
            filename = previous_step_output.default_cache_key(
                self.name,
                self.version,
            )

        if store.file_exists(filename):
            self.logger.info(f"Cache hit:  {filename}")
            return filename

        self.logger.info(f"Cache miss: {filename}")

        return None

    def run(
        self,
        previous_step_output: list[DataModel] | None = None,
    ) -> list[Out | list[Out]]:
        assert previous_step_output is not None

        for item in previous_step_output:
            assert isinstance(item, self.input_type)

        return self.transform_many(list(previous_step_output))  # type: ignore[arg-type]


class BaseGatherTransformer[
    In: DataModel,
    Out: DataModel,
](
    ProcessingStep[Out],
    Writer[Out],
    Reader[In],
    ABC,
):
    """Collects the parts a `fan_out` transformer split one input into and emits one output."""

    def __init__(
        self,
        logger: Logger,
    ):
        self.logger = logger

    @abstractmethod
    def gather(self, parts: list[In]) -> Out:
        """`parts` are in the order the fan-out step emitted them."""

    def check_cache(
        self,
        store: BlobStore,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> str | None:
        assert isinstance(previous_step_output, list)
        filename = DataModel.cache_list(self.name, previous_step_output, self.version)

        if store.file_exists(filename):
            self.logger.info(f"Cache hit:  {filename}")
            return filename

        self.logger.info(f"Cache miss: {filename}")

        return None

    def run(
        self,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> Out:
        assert isinstance(previous_step_output, list)

        return self.gather(previous_step_output)  # type: ignore[arg-type]


TRANSFORMER_TYPES = (BaseTransformer, BaseStreamingTransformer, BaseGatherTransformer)


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


class BasePydanticGatherTransformer[
    In: DataModel,
    Out: DataModel,
](
    BaseGatherTransformer[
        In,
        Out,
    ],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass
