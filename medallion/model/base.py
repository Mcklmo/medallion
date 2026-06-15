from abc import ABC, abstractmethod
from io import BytesIO
import json
from typing import Any, Iterable, TypeVar, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field
from logging import Logger

from medallion.store.base import BlobStore
import hashlib


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


class DataModel(BaseModel):
    input_content_hash: str | None = Field(
        description="Hash of the input content used to generate this output, used for caching purposes",
        default=None,
    )

    def hash(self, _content: bytes) -> str:
        hasher = hashlib.sha256()
        content = BytesIO(_content)

        for chunk in iter(lambda: content.read(8192), b""):
            hasher.update(chunk)

        return hasher.hexdigest()

    def stringify(self) -> bytes:
        """Overwrite this to control the hash used for caching"""
        return self.model_dump_json().encode()

    def create_cache_key(
        self,
        transformer_name: str,
        hash: str,
    ) -> str:
        return f"cache/{transformer_name}/{hash}"

    def default_cache_key(self, transformer_name: str) -> str:
        return self.create_cache_key(
            transformer_name, self.input_content_hash or self.hash(self.stringify())
        )


class FileOutput(DataModel):
    model_config = ConfigDict(
        ser_json_bytes="base64",
        val_json_bytes="base64",
    )

    content: bytes
    is_full_file: bool = Field(
        alias="is_full_file",
        default=True,
    )

    def stringify(self) -> bytes:
        return self.content


class ProcessingStep[Out](ABC):
    @abstractmethod
    def write_output(self, output_data: Out) -> BytesIO | list[BytesIO]:
        pass

    @property
    def name(self) -> str:
        return self.__class__.__name__


class Writer[Out](ABC):
    @classproperty
    def output_type(cls) -> type:
        return _resolve_type_arg(cast(type, cls), Writer, 0)

    @abstractmethod
    def load_cached(self, data: BytesIO) -> Out:
        """Used for loading cached data"""
        pass

    @abstractmethod
    def check_cache(
        self,
        store: BlobStore,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> str | None:
        pass

    @abstractmethod
    def run(
        self, previous_step_output: DataModel | list[DataModel] | None = None
    ) -> Iterable[Out] | Out:
        pass

    def load_cache_or_run(
        self,
        store: BlobStore,
        logger: Logger,
        force_run: bool,
        name: str,
        previous_step_output: DataModel | list[DataModel] | None = None,
    ) -> Iterable[Out] | Out:
        previous_run_filename: str | None = None

        if not force_run:
            logger.info(f"Checking for cached output in folder[{name}]...")
            previous_run_filename = self.check_cache(
                store,
                previous_step_output,
            )

        if not previous_run_filename or force_run:
            if force_run:
                logger.info("Skipping cache and forcing a run.")
            else:
                logger.info("Cache miss")

            return self.run(previous_step_output)

        files_at_path = store.list_files_at(previous_run_filename)
        logger.info(
            f"Cache hit, loading output from: {previous_run_filename}, with {len(files_at_path)} files",
        )

        items: list[Out] = []

        for filename in files_at_path:
            downloaded_file = store.download_file(filename)
            output = self.load_cached(
                downloaded_file,
            )

            if len(files_at_path) == 1:
                return output

            items.append(output)

        return items


class Reader[In: DataModel](ABC):
    @classproperty
    def input_type(cls) -> type:
        return _resolve_type_arg(cast(type, cls), Reader, 0)

    @abstractmethod
    def read_input_bytes(self, data: bytes) -> In:
        pass


class FileReader(Reader[FileOutput], ABC):
    def read_input_bytes(self, data: bytes) -> FileOutput:
        return FileOutput.model_validate_json(data)


class PydanticReader[In: DataModel](Reader[In], ABC):
    def read_input_bytes(self, data: bytes) -> In:
        schema: In = self.input_type
        return cast(In, schema.model_validate_json(data))


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
    Writer[Out],
    ABC,
):
    def load_cached(self, data: BytesIO | bytes) -> Out:
        if isinstance(data, BytesIO):
            byte_data = data.read()
        else:
            assert isinstance(data, bytes), "i hate dynamically typed languages"
            byte_data = data

        schema = self.output_type

        return cast(
            Out,
            schema.model_validate_json(byte_data),
        )

    def write_output(self, output_data: Out) -> BytesIO:
        data: Out | Iterable[Out] = output_data

        if isinstance(data, self.output_type):
            return BytesIO(
                data.model_dump_json(
                    indent=2,
                    by_alias=True,
                ).encode()
            )

        assert isinstance(data, Iterable)

        __output_data = list(data)

        _output_data: list[
            dict[
                str,
                Any,
            ]
        ] = [
            item.model_dump_json(
                by_alias=True,
            )
            for item in __output_data
        ]
        serialized_output = json.dumps(_output_data, indent=2)

        return BytesIO(serialized_output.encode())

    @property
    def header_line(self) -> bytes:
        field_names = self.output_type.model_fields.keys()
        return (",".join(field_names) + "\n").encode()

    def write_row(self, output_data: Out) -> BytesIO:
        field_names = self.output_type.model_fields.keys()
        row_values = [str(getattr(output_data, field)) for field in field_names]

        return BytesIO((",".join(row_values) + "\n").encode())
