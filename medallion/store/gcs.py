from google.cloud import storage  # type: ignore[attr-defined]
from google.oauth2 import service_account

from io import BytesIO

from medallion.store.base import BlobStore


class GCStorage(BlobStore):
    def __init__(
        self,
        credentials: service_account.Credentials,
        bucket_name: str,
    ):
        self.storage_client = storage.Client(
            credentials=credentials,
        )
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload_file(
        self,
        destination_path: str,
        content: BytesIO,
    ) -> None:
        blob = self.bucket.blob(destination_path)
        blob.upload_from_file(content)

    def download_file(self, path: str) -> BytesIO:
        blob = self.bucket.blob(path)
        return BytesIO(blob.download_as_bytes())

    def list_files_at(
        self,
        prefix: str,
        suffix: str | None = None,
    ) -> list[str]:
        blobs = self.storage_client.list_blobs(
            self.bucket.name,
            prefix=prefix,
        )
        paths = [blob.name for blob in blobs]
        if suffix is not None:
            paths = [p for p in paths if p.endswith(suffix)]

        return paths
