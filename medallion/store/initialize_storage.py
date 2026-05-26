from medallion.store.base import (
    FILE_STORAGE_TYPE_ENV_VAR,
    BlobStore,
    load_service_account_credentials,
    must_get_env,
)


from logging import Logger

from medallion.store.gcs import GCStorage
from medallion.store.local import LocalStorage


def initialize_storage(
    output_dir: str,
    logger: Logger,
) -> "BlobStore":
    file_storage_type = must_get_env(FILE_STORAGE_TYPE_ENV_VAR)
    if file_storage_type == "local":
        return LocalStorage(
            output_dir=output_dir,
            logger=logger,
        )

    assert (
        file_storage_type == "gcs"
    ), f"Unsupported FILE_STORAGE_TYPE: {file_storage_type}"

    return GCStorage(
        credentials=load_service_account_credentials(),
        bucket_name=must_get_env("GCS_BUCKET"),
    )
