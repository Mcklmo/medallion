import pendulum
from google.cloud import storage
from google.oauth2 import service_account

from io import BytesIO

from medallion.store.base import BlobStore, SourceDocumentLocation


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

    def get_file_location(self, relative_path: str) -> SourceDocumentLocation:
        blob = self.bucket.blob(relative_path)
        assert blob.exists()

        signed = blob.generate_signed_url(
            expiration=pendulum.now().add(hours=1).int_timestamp
        )  # URL valid for 1 hour
        return SourceDocumentLocation(file_url=signed)

    def file_exists(self, destination_path: str) -> bool:
        blob = self.bucket.blob(destination_path)
        return blob.exists()

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
