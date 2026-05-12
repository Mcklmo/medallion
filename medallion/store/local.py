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

    def list_files_with_prefix(
        self,
        prefix: str,
        suffix: str,
    ) -> list[str]:
        if not os.path.exists(self.output_dir):
            return []

        files = []
        for entry in os.listdir(self.output_dir):
            entry_path = os.path.join(self.output_dir, entry)
            if not os.path.isdir(entry_path):
                continue

            if not entry.startswith(prefix):
                continue

            for root, _, filenames in os.walk(entry_path):
                for filename in filenames:
                    relative_path = os.path.relpath(
                        os.path.join(root, filename),
                        self.output_dir,
                    )
                    if relative_path.endswith(suffix):
                        files.append(relative_path)

        return files

    def list_files_at(
        self,
        prefix: str,
        suffix: str,
    ) -> list[str]:
        dir_path = os.path.join(self.output_dir, prefix)
        if not os.path.exists(dir_path):
            return []

        files = []
        for root, _, filenames in os.walk(dir_path):
            for filename in filenames:
                relative_path = os.path.relpath(
                    os.path.join(root, filename),
                    self.output_dir,
                )
                if relative_path.endswith(suffix):
                    files.append(relative_path)

        return files
