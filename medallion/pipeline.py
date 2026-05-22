import hashlib
from io import BytesIO
from logging import Logger
from typing import Any, Iterable, Optional
import pendulum
from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from pydantic import BaseModel, ConfigDict
from medallion.store.base import BlobStore


def compute_content_hash(
    content: BytesIO | list[BytesIO],
) -> str:
    hasher = hashlib.sha256()
    CHUNK_SIZE = 8 * 1024  # 8 KB

    if isinstance(content, list):
        for c in content:
            c.seek(0)
            for chunk in iter(
                lambda: c.read(CHUNK_SIZE),
                b"",
            ):
                hasher.update(chunk)
            c.seek(0)
    else:
        content.seek(0)
        for chunk in iter(
            lambda: content.read(CHUNK_SIZE),
            b"",
        ):
            hasher.update(chunk)
        content.seek(0)

    return hasher.hexdigest()


EXTRACTOR_TYPE_ASSERTION_MESSAGE = (
    f"First class must be of type {BaseExtractor.__name__}"
)


class PipeLine(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
    )
    extractor: BaseExtractor
    transformers: Optional[list[BaseTransformer | BaseStreamingTransformer]]
    logger: Logger
    store_output: BlobStore
    store_cache: BlobStore

    def get_name(self) -> str:
        return "_".join(
            [self.extractor.__class__.__name__]
            + [t.__class__.__name__ for t in self.transformers or []]
        )

    def run(
        self,
        force_run_extractor: bool = False,
    ) -> Any:
        extractor = self.extractor
        pipe_name = self.get_name()
        self.logger.info(f"Starting pipeline execution: {pipe_name}")
        i = 0
        start_time = pendulum.now().format("YYYY-MM-DD-HH-mm-ssSSS")
        filename = f"{i}_{extractor.name}.{extractor.file_extension}"

        output_previous_bytes = None
        output_previous = None

        if not force_run_extractor:
            self.logger.info("Checking for last run's extractor output")
            # check a previous run for "cache" hit before running the extractor
            output_previous_bytes = (
                self.find_latest_extractor_file_with_prefix_and_suffix(
                    extractor.name,
                    pipe_name,
                    filename,
                )
            )
            if output_previous_bytes:
                self.logger.warning(
                    "Found previous extractor output, using it instead of running extractor"
                )
                output_previous = extractor.read_bytes(output_previous_bytes)
        else:
            self.logger.info("Force run extractor enabled, skipping cache check")

        if output_previous_bytes is None:
            self.logger.info("Running extractor")

            output_previous = extractor.extract()
            output_previous_bytes = extractor.write_output(output_previous)

        self.upload_extractor_output(
            extractor.name,
            extractor.file_extension,
            pipe_name,
            i,
            start_time,
            filename,
            output_previous_bytes,
        )

        i += 1

        for t in self.transformers or []:
            output_previous = self.execute_transformer_with_cache(
                pipe_name,
                i,
                start_time,
                output_previous_bytes,
                output_previous,
                t,
            )

            i += 1

        return output_previous

    def find_latest_extractor_file_with_prefix_and_suffix(
        self,
        extractor_name: str,
        filename_prefix: str,
        filename_suffix: str,
    ) -> BytesIO | None:
        """If any files exist with extractor_name_ prefix and filename_suffix, or filename_prefix and filename_suffix, return the latest one as bytes. Otherwise, return None."""
        dir_content = self.store_output.list_files_with_prefix(
            extractor_name
            + "_",  # underscore somewhat ensures this pipe started with exactly this extractor.
            filename_suffix,
        ) + self.store_output.list_files_at(
            filename_prefix,
            filename_suffix,
        )

        if not dir_content:
            return None

        latest_file = sorted(dir_content)[-1]

        return self.store_output.download_file(latest_file)

    def execute_transformer_with_cache(
        self,
        pipe_name: str,
        i: int,
        start_time: str,
        output_previous_bytes: BytesIO,
        output_previous: Any,
        t: BaseTransformer | BaseStreamingTransformer,
    ) -> Any:
        content_hash = compute_content_hash(output_previous_bytes)
        cache_path = f"{content_hash}/{t.name}.{t.file_extension}"

        if self.store_cache.file_exists(cache_path):
            self.logger.info(f"Cache hit for transformer {t.name}: {cache_path}")

            output_previous_bytes = self.store_cache.download_file(cache_path)
            output_previous = t.read_bytes(output_previous_bytes)
        else:
            self.logger.info(
                f"Cache miss for transformer {t.name}. Caching result at {cache_path}"
            )

            if isinstance(
                t,
                BaseTransformer,
            ) or isinstance(
                output_previous,
                Iterable,
            ):
                output_previous = t.transform(output_previous)
            else:
                assert isinstance(
                    t, BaseStreamingTransformer
                ), f"Transformer must be of type {BaseTransformer.__name__} or {BaseStreamingTransformer.__name__}"

                output_previous = t.transform_one(output_previous)

            output_previous_bytes = t.write_output(output_previous)

            self.store_cache.upload_file(
                destination_path=cache_path,
                content=output_previous_bytes,
            )

        self.store_output.upload_file(
            destination_path=f"{pipe_name}/{start_time}/{i}_{t.name}.{t.file_extension}",
            content=output_previous_bytes,
        )

        return output_previous

    def upload_extractor_output(
        self,
        processor_name: str,
        processor_file_extension: str,
        pipe_name: str,
        i: int,
        start_time: str,
        filename: str,
        output_previous_bytes: BytesIO | Iterable[BytesIO],
    ) -> None:
        if isinstance(output_previous_bytes, BytesIO):
            filename_output = f"{pipe_name}/{start_time}/{filename}"

            self.store_output.upload_file(
                destination_path=filename_output,
                content=output_previous_bytes,
            )

            return

        assert isinstance(
            output_previous_bytes,
            Iterable,
        ), "Output must be either BytesIO or Iterable[BytesIO]"

        dirname = f"{pipe_name}/{start_time}"

        for j, file in enumerate(output_previous_bytes):
            filename = f"{i}/{j}_{processor_name}.{processor_file_extension}"
            self.store_output.upload_file(
                destination_path=f"{dirname}/{filename}",
                content=file,
            )

    def model_post_init(self, context: Any) -> None:
        previous_output_type = self.extractor.output_type

        for t in self.transformers or []:
            assert isinstance(
                t,
                (BaseTransformer, BaseStreamingTransformer),
            ), f"Transformers must be of type {BaseTransformer.__name__} or {BaseStreamingTransformer.__name__}"

            assert t.input_type == previous_output_type, f"""\
                Transformer {t.__class__.__name__} expects input of type {t.input_type}, \
                but previous output is of type {previous_output_type}\
            """
            previous_output_type = t.output_type
