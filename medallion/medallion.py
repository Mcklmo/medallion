import hashlib
from io import BytesIO
from logging import Logger

import pendulum


from medallion.log import create_logger
from medallion.resolve_classes import load_classes_from_user_input
from medallion.store.store import initialize_storage, must_get_env


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


def medallion(logger: Logger) -> None:
    store_output = initialize_storage(
        must_get_env("LOCAL_OUTPUT_DIR"),
        logger,
    )
    store_cache = initialize_storage(
        must_get_env("LOCAL_CACHE_DIR"),
        logger,
    )
    pipe = load_classes_from_user_input()
    extractor = pipe.extractor
    pipe_name = pipe.get_name()
    logger.info(f"Starting pipeline execution: {pipe_name}")
    i = 0
    start_time = pendulum.now().format("YYYY-MM-DD-HH-mm-ssSSS")
    filename = f"{i}_{extractor.name}.{extractor.file_extension}"
    filename_output = f"{pipe_name}/{start_time}/{filename}"

    # check a previous run for "cache" hit before running the extractor
    dir_content = store_output.list_files_at(
        pipe_name,
        filename,
    )
    if dir_content:
        latest_file = sorted(dir_content)[-1]
        logger.info(f"Cache hit for extractor from previous run: {latest_file}")
        output_previous_bytes = store_output.download_file(latest_file)
        output_previous = extractor.read_bytes(output_previous_bytes)
    else:
        logger.info("Cache miss for extractor, running extraction.")
        output_previous = extractor.extract()
        output_previous_bytes = extractor.write_output(output_previous)

    store_output.upload_file(
        destination_path=filename_output,
        content=output_previous_bytes,
    )
    i += 1

    for t in pipe.transformers or []:
        content_hash = compute_content_hash(output_previous_bytes)
        cache_path = f"{content_hash}.{t.file_extension}"

        if store_cache.file_exists(cache_path):
            logger.info(f"Cache hit for transformer {t.name}: {cache_path}")

            output_previous_bytes = store_cache.download_file(cache_path)
            output_previous = t.read_bytes(output_previous_bytes)
        else:
            logger.info(
                f"Cache miss for transformer {t.name}. Caching result at {cache_path}"
            )

            output_previous = t.transform(output_previous)
            output_previous_bytes = t.write_output(output_previous)

            store_cache.upload_file(
                destination_path=cache_path,
                content=output_previous_bytes,
            )

        store_output.upload_file(
            destination_path=f"{pipe_name}/{start_time}/{i}_{t.name}.{t.file_extension}",
            content=output_previous_bytes,
        )

        i += 1


def main() -> None:
    medallion(create_logger())


if __name__ == "__main__":
    main()
