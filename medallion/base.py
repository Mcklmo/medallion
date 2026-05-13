from abc import ABC, abstractmethod
from io import BytesIO
import json
from typing import TypeVar, get_args, get_origin

from pydantic import BaseModel


def _resolve_type_arg(instance: object, base: type, index: int) -> type:
    def walk(cls: type, subs: dict) -> type | None:
        for orig in getattr(cls, "__orig_bases__", ()):
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

    result = walk(type(instance), {})
    assert result is not None, (
        f"{type(instance).__name__} must subclass {base.__name__}[...] "
        f"with type arguments"
    )

    return result


class ProcessingStep[Out](ABC):
    @property
    @abstractmethod
    def file_extension(self) -> str:
        pass

    @property
    @abstractmethod
    def queue_to(self) -> str:
        pass

    @abstractmethod
    def write_output(self, output_data: Out) -> BytesIO:
        pass

    @property
    def name(self) -> str:
        return self.__class__.__name__


class BaseExtractor[Out](ProcessingStep[Out], ABC):
    @property
    def output_type(self) -> type:
        return _resolve_type_arg(self, BaseExtractor, 0)

    @abstractmethod
    def extract(self) -> Out:
        pass

    @abstractmethod
    def read_bytes(self, data: BytesIO) -> Out:
        pass


class BaseTransformer[In, Out](ProcessingStep[Out], ABC):
    @property
    @abstractmethod
    def queue_from(self) -> str:
        pass

    @abstractmethod
    def transform(self, data: In) -> Out:
        pass

    @abstractmethod
    def read_bytes(self, data: BytesIO) -> Out:
        pass

    @property
    def input_type(self) -> type:
        return _resolve_type_arg(self, BaseTransformer, 0)

    @property
    def output_type(self) -> type:
        return _resolve_type_arg(self, BaseTransformer, 1)


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


class BasePydanticTransformer[
    In,
    Out: BaseModel,
](BaseTransformer[In, Out], ABC):

    def read_bytes(self, data: BytesIO) -> Out:
        byte_data = data.read()
        return self.output_type.model_validate_json(byte_data)

    @property
    def file_extension(self):
        return "json"

    def write_output(self, output_data: Out) -> BytesIO:
        return BytesIO(output_data.model_dump_json(indent=2).encode())
