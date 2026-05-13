import hashlib
from io import BytesIO
from logging import Logger
from typing import Any, Optional
import pendulum
from medallion.base import BaseExtractor, BaseTransformer
from pydantic import BaseModel, ConfigDict
from medallion.store.base import BlobStore


def compute_content_hash(content: BytesIO) -> str:
    hasher = hashlib.sha256()
    CHUNK_SIZE = 8 * 1024  # 8 KB

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
    transformers: Optional[list[BaseTransformer]]
    logger: Logger
    store_output: BlobStore
    store_cache: BlobStore

    def get_name(self) -> str:
        return "_".join(
            [self.extractor.__class__.__name__]
            + [t.__class__.__name__ for t in self.transformers or []]
        )

    def run(self) -> any:
        extractor = self.extractor
        pipe_name = self.get_name()
        self.logger.info(f"Starting pipeline execution: {pipe_name}")
        i = 0
        start_time = pendulum.now().format("YYYY-MM-DD-HH-mm-ssSSS")
        filename = f"{i}_{extractor.name}.{extractor.file_extension}"
        filename_output = f"{pipe_name}/{start_time}/{filename}"

        # check a previous run for "cache" hit before running the extractor
        dir_content = self.store_output.list_files_with_prefix(
            extractor.name
            + "_",  # underscore somewhat ensures this pipe started with exactly this extractor.
            filename,
        ) + self.store_output.list_files_at(
            pipe_name,
            filename,
        )
        if dir_content:
            latest_file = sorted(dir_content)[-1]
            self.logger.info(
                f"Cache hit for extractor from previous run: {latest_file}"
            )

            output_previous_bytes = self.store_output.download_file(latest_file)
            output_previous = extractor.read_bytes(output_previous_bytes)
        else:
            self.logger.info("Cache miss for extractor, running extraction.")

            output_previous = extractor.extract()
            output_previous_bytes = extractor.write_output(output_previous)

        self.store_output.upload_file(
            destination_path=filename_output,
            content=output_previous_bytes,
        )
        i += 1

        for t in self.transformers or []:
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

                output_previous = t.transform(output_previous)
                output_previous_bytes = t.write_output(output_previous)

                self.store_cache.upload_file(
                    destination_path=cache_path,
                    content=output_previous_bytes,
                )

            self.store_output.upload_file(
                destination_path=f"{pipe_name}/{start_time}/{i}_{t.name}.{t.file_extension}",
                content=output_previous_bytes,
            )

            i += 1

        return output_previous

    def model_post_init(self, context: Any) -> None:
        previous_output_type = self.extractor.output_type

        for t in self.transformers or []:
            assert isinstance(
                t,
                BaseTransformer,
            ), f"Transformers must be of type {BaseTransformer.__name__}"

            assert t.input_type == previous_output_type, f"""\
                Transformer {t.__class__.__name__} expects input of type {t.input_type}, \
                but previous output is of type {previous_output_type}\
            """
            previous_output_type = t.output_type
