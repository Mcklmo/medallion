from pydantic import BaseModel

from medallion.model.base import (
    BaseJSONStep,
    BasePydanticProcessingStep,
    ProcessingStep,
    Writer,
)


from abc import ABC, abstractmethod
from typing import Iterator


class BaseExtractor[Out](
    ProcessingStep[Out],
    Writer[Out],
    ABC,
):
    @abstractmethod
    def extract(self) -> Iterator[Out]:
        pass


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
