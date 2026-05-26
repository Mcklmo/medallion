from google.cloud import storage
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
