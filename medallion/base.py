from abc import ABC, abstractmethod
from typing import get_args, get_origin


def _resolve_type_arg(instance: object, base: type, index: int) -> type:
    for orig in type(instance).__orig_bases__:
        if get_origin(orig) is base:
            return get_args(orig)[index]
    raise TypeError(
        f"{type(instance).__name__} must subclass {base.__name__}[...] "
        f"with type arguments"
    )


class BaseExtractor[Out](ABC):
    @property
    def output_type(self) -> type:
        return _resolve_type_arg(self, BaseExtractor, 0)

    @abstractmethod
    def extract(self) -> Out:
        pass


class BaseTransformer[In, Out](ABC):
    @property
    def input_type(self) -> type:
        return _resolve_type_arg(self, BaseTransformer, 0)

    @property
    def output_type(self) -> type:
        return _resolve_type_arg(self, BaseTransformer, 1)

    @abstractmethod
    def transform(self, data: In) -> Out:
        pass
