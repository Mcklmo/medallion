from abc import ABC, abstractmethod
from io import BytesIO
import json
from typing import TypeVar, get_args, get_origin

from pydantic import BaseModel


class classproperty:
    """Read-only descriptor that resolves on both the class and instances."""

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, owner):
        return self.func(owner)


def _resolve_type_arg(cls: type, base: type, index: int) -> type:
    def walk(klass: type, subs: dict) -> type | None:
        for orig in getattr(klass, "__orig_bases__", ()):
            origin = get_origin(orig)
            if origin is None:
                continue

            resolved_args = tuple(
                subs.get(a, a) if isinstance(a, TypeVar) else a for a in get_args(orig)
            )

            if origin is base:
                return resolved_args[index]

            parent_subs = dict(zip(origin.__parameters__, resolved_args))

            found = walk(origin, parent_subs)
            if found is not None:
                return found

        return None

    result = walk(cls, {})
    assert result is not None, (
        f"{cls.__name__} must subclass {base.__name__}[...] " f"with type arguments"
    )

    return result


class ProcessingStep[Out](ABC):
    @property
    @abstractmethod
    def file_extension(self) -> str:
        pass

    @abstractmethod
    def write_output(self, output_data: Out) -> BytesIO:
        pass

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abstractmethod
    def read_bytes(self, data: BytesIO) -> Out:
        pass


class BaseExtractor[Out](ProcessingStep[Out], ABC):
    @classproperty
    def output_type(cls) -> type:
        return _resolve_type_arg(cls, BaseExtractor, 0)

    @abstractmethod
    def extract(self) -> Out:
        pass


class BaseTransformer[In, Out](ProcessingStep[Out], ABC):
    @abstractmethod
    def transform(self, data: In) -> Out:
        pass

    @classproperty
    def input_type(cls) -> type:
        return _resolve_type_arg(cls, BaseTransformer, 0)

    @classproperty
    def output_type(cls) -> type:
        return _resolve_type_arg(cls, BaseTransformer, 1)


class BaseJSONStep[Out](ProcessingStep[Out]):
    @property
    def file_extension(self) -> str:
        return "json"

    def write_output(self, output_data: Out) -> BytesIO:
        output_stream = BytesIO()
        output_stream.write(json.dumps(output_data).encode())
        output_stream.seek(0)

        return output_stream


class BaseJSONTransformer[In, Out](BaseTransformer[In, Out], BaseJSONStep[Out], ABC):
    def read_bytes(self, data: BytesIO) -> Out:
        data.seek(0)
        return json.loads(data.read().decode())


class BaseJSONExtractor[Out](BaseExtractor[Out], BaseJSONStep[Out], ABC):
    pass


class BasePydanticProcessingStep[
    Out: BaseModel,
](ProcessingStep[Out], ABC):
    def read_bytes(self, data: BytesIO) -> list[Out]:
        byte_data = data.read()
        result: list[Out] = []
        for item in json.loads(byte_data):
            result.append(self.output_type.model_validate(item))

        return result

    def write_output(self, output_data: Out) -> BytesIO:
        return BytesIO(output_data.model_dump_json(indent=2).encode())

    @property
    def file_extension(self) -> str:
        return "json"


class BasePydanticExtractor[
    Out: BaseModel,
](
    BaseExtractor[Out],
    BasePydanticProcessingStep[Out],
    ABC,
):
    pass


class BasePydanticTransformer[
    In,
    Out: BaseModel,
](
    BaseTransformer[In, Out],
    BasePydanticProcessingStep,
    ABC,
):
    pass
