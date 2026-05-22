from logging import Logger
from os import getenv
from google.oauth2 import service_account
from medallion.store.base import BlobStore
from medallion.store.gcs import GCStorage
from medallion.store.local import LocalStorage


def must_get_env(key: str) -> str:
    value = getenv(key)
    assert value is not None, f"Required environment variable {key} is missing."

    return value


FILE_STORAGE_TYPE_ENV_VAR = "FILE_STORAGE_TYPE"


def initialize_storage(
    output_dir: str,
    logger: Logger,
) -> BlobStore:
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


def load_service_account_credentials():
    return service_account.Credentials.from_service_account_file(
        must_get_env("GOOGLE_APPLICATION_CREDENTIALS")
    )
