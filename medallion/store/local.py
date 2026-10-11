import logging
import os
import uuid
from io import BytesIO
from medallion.store.base import BlobStore


class LocalStorage(BlobStore):
    def __init__(
        self,
        output_dir: str,
        logger: logging.Logger,
    ):
        self.output_dir = output_dir
        self.logger = logger

    def file_exists(self, destination_path: str) -> bool:
        dest_path = os.path.join(self.output_dir, destination_path)
        return os.path.exists(dest_path)

    def upload_file(
        self,
        destination_path: str,
        content: BytesIO,
    ) -> None:
        dest_path = os.path.join(
            self.output_dir,
            destination_path,
        )
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        # write then rename, so a reader never sees a created but still empty file
        tmp_path = os.path.join(
            os.path.dirname(dest_path),
            f".{os.path.basename(dest_path)}.{uuid.uuid4().hex}",
        )
        with open(tmp_path, "xb") as dst:
            dst.write(content.getvalue())

        os.replace(tmp_path, dest_path)

        self.logger.debug(f"Uploaded file to {dest_path}")

    def download_file(self, path: str) -> BytesIO:
        full_path = os.path.join(self.output_dir, path)
        with open(full_path, "rb") as f:
            return BytesIO(f.read())

    def list_files_at(
        self,
        prefix: str,
        suffix: str | None = None,
    ) -> list[str]:
        dir_path = os.path.join(self.output_dir, prefix)
        if not os.path.exists(dir_path):
            return []

        files = []
        for root, _, filenames in os.walk(dir_path):
            for filename in filenames:
                if filename.startswith("."):  # upload_file's files mid-write
                    continue

                relative_path = os.path.relpath(
                    os.path.join(root, filename),
                    self.output_dir,
                )
                if not suffix or relative_path.endswith(suffix):
                    files.append(relative_path)

        return files
