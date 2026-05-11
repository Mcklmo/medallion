from logging import Logger
from os import getenv
from google.oauth2 import service_account
from medallion.store.gcs import GCStorage
from medallion.store.local import LocalStorage


def must_get_env(key: str) -> str:
    value = getenv(key)
    assert value is not None, f"Environment variable {key} is required but not set."

    return value


def initialize_storage(
    output_dir: str,
    logger: Logger,
) -> GCStorage | LocalStorage:
    _storage: GCStorage | LocalStorage

    file_storage_type = must_get_env("FILE_STORAGE_TYPE")
    if file_storage_type == "local":
        _storage = LocalStorage(
            output_dir=output_dir,
            logger=logger,
        )
    elif file_storage_type == "gcs":
        _storage = GCStorage(
            credentials=service_account.Credentials.from_service_account_file(
                must_get_env("GOOGLE_APPLICATION_CREDENTIALS")
            ),
            bucket_name=must_get_env("GCS_BUCKET"),
        )
    else:
        raise ValueError(f"Unsupported FILE_STORAGE_TYPE: {file_storage_type}")

    return _storage
