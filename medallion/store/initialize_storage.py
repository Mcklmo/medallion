from medallion.store.base import (
    FILE_STORAGE_TYPE_ENV_VAR,
    BlobStore,
    get_env_or_default,
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
    local_output_dir: (
        str | None
    ) = None,  # can be overridden to create multiple stores with multiple output folder locations
) -> "BlobStore":
    file_storage_type = must_get_env(FILE_STORAGE_TYPE_ENV_VAR)
    if file_storage_type == FILE_STORAGE_LOCAL:
        output_dir = (
            get_env_or_default(
                LOCAL_OUTPUT_DIR_ENV_VAR,
                ".medallion-data",
            )
            if local_output_dir is None
            else local_output_dir
        )

        return LocalStorage(
            output_dir=output_dir,
            logger=logger,
        )

    assert (
        file_storage_type == "gcs"
    ), f"Unsupported FILE_STORAGE_TYPE: {file_storage_type}"

    return GCStorage(
        credentials=load_service_account_credentials(),
        bucket_name=must_get_env(GCS_BUCKET_ENV_VAR),
    )
