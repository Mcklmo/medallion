from medallion.store.base import (
    FILE_STORAGE_TYPE_ENV_VAR,
    BlobStore,
    load_service_account_credentials,
    must_get_env,
)


from logging import Logger

from medallion.store.gcs import GCStorage
from medallion.store.local import LocalStorage

LOCAL_OUTPUT_DIR_ENV_VAR = "LOCAL_OUTPUT_DIR"
GCS_BUCKET_ENV_VAR = "GCS_BUCKET"
FILE_STORAGE_LOCAL = "local"


def initialize_storage(
    logger: Logger,
    local_output_dir: str | None = None,
) -> "BlobStore":
    file_storage_type = must_get_env(FILE_STORAGE_TYPE_ENV_VAR)
    if file_storage_type == FILE_STORAGE_LOCAL:
        return LocalStorage(
            output_dir=(
                must_get_env(LOCAL_OUTPUT_DIR_ENV_VAR)
                if local_output_dir is None
                else local_output_dir
            ),
            logger=logger,
        )

    assert (
        file_storage_type == "gcs"
    ), f"Unsupported FILE_STORAGE_TYPE: {file_storage_type}"

    return GCStorage(
        credentials=load_service_account_credentials(),
        bucket_name=must_get_env(GCS_BUCKET_ENV_VAR),
    )
