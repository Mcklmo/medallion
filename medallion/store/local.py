import logging
import os
from io import BytesIO
from medallion.store.base import BlobStore, SourceDocumentLocation


class LocalStorage(BlobStore):
    def __init__(
        self,
        output_dir: str,
        logger: logging.Logger,
    ):
        self.output_dir = output_dir
        self.logger = logger

    def get_file_location(self, relative_path: str) -> SourceDocumentLocation:
        full_path = os.path.join(self.output_dir, relative_path)
        assert os.path.exists(full_path), f"File not found at {full_path}"

        return SourceDocumentLocation(file_local_path=full_path)

    def file_exists(self, destination_path: str) -> bool:
        dest_path = os.path.join(self.output_dir, destination_path)
        return os.path.exists(dest_path)

    def upload_file(
        self,
        destination_path: str,
        content: BytesIO,
    ) -> None:
        dest_path = os.path.join(self.output_dir, destination_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        with open(dest_path, "wb") as dst:
            dst.write(content.getvalue())

    def download_file(self, path: str) -> BytesIO:
        full_path = os.path.join(self.output_dir, path)
        with open(full_path, "rb") as f:
            return BytesIO(f.read())
