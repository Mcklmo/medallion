from abc import ABC, abstractmethod
from io import BytesIO
import json
from typing import Generator, Iterator, TypeVar, get_args, get_origin

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

            parent_subs = dict(
                zip(
                    getattr(origin, "__parameters__", ()),
                    resolved_args,
                )
            )

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


class Writer[Out](ABC):
    @classproperty
    def output_type(cls) -> type:
        return _resolve_type_arg(cls, Writer, 0)

    @abstractmethod
    def read_bytes(self, data: BytesIO) -> Out:
        """Used for loading cached data"""
        pass


class Reader[In](ABC):
    @classproperty
    def input_type(cls) -> type:
        return _resolve_type_arg(cls, Reader, 0)

    @abstractmethod
    def read_input_bytes(self, data: BytesIO) -> In:
        pass


class BaseJSONStep[Out](ProcessingStep[Out]):
    @property
    def file_extension(self) -> str:
        return "json"

    def write_output(self, output_data: Out) -> BytesIO:
        output_stream = BytesIO()
        output_stream.write(json.dumps(output_data).encode())
        output_stream.seek(0)

        return output_stream


class BasePydanticProcessingStep[
    Out: BaseModel,
](
    ProcessingStep[Out],
    ABC,
):
    def read_bytes(self, data: BytesIO | bytes) -> Out:
        if isinstance(data, BytesIO):
            byte_data = data.read()
        else:
            assert isinstance(data, bytes), "i hate dynamically typed languages"
            byte_data = data

        schema = self.output_type

        return [schema.model_validate(item) for item in json.loads(byte_data.decode())]

    def write_output(self, output_data: Out) -> BytesIO:
        if isinstance(output_data, Generator):
            output_data = list(output_data)

        if isinstance(output_data, Iterator):
            output_data = list(output_data)

        if isinstance(output_data, list):
            output_data: list[BaseModel] = output_data
            _output_data = [item.model_dump() for item in output_data]
            return BytesIO(json.dumps(_output_data, indent=2).encode())

        return BytesIO(output_data.model_dump_json(indent=2).encode())

    @property
    def file_extension(self) -> str:
        return "json"

    @property
    def header_line(self) -> bytes:
        field_names = self.output_type.model_fields.keys()
        return (",".join(field_names) + "\n").encode()

    def write_row(self, output_data: Out) -> BytesIO:
        field_names = self.output_type.model_fields.keys()
        row_values = [str(getattr(output_data, field)) for field in field_names]

        return BytesIO((",".join(row_values) + "\n").encode())
