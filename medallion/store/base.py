from abc import ABC, abstractmethod
from io import BytesIO
import pendulum
from pydantic import BaseModel, Field, model_validator
from typing import Optional
from typing_extensions import Self


class SourceDocumentLocation(BaseModel):
    file_url: Optional[str] = Field(default=None)
    file_local_path: Optional[str] = Field(default=None)

    @model_validator(mode="after")
    def check_passwords_match(self) -> Self:
        assert (
            self.file_url or self.file_local_path
        ), "Must provide either file_url or file_local_path"
        assert not (
            self.file_url and self.file_local_path
        ), "Cannot provide both file_url and file_local_path"

        return self


FOLDERNAME_DATETIME_FORMAT = "YYYY-MM-DDTHH-mm-ssSSS"


class BlobStore(ABC):
    @abstractmethod
    def get_file_location(
        self,
        relative_path: str,
    ) -> SourceDocumentLocation:
        """Given a relative path, return either a URL or local file path to the source document."""
        pass

    @abstractmethod
    def file_exists(self, destination_path: str) -> bool:
        pass

    @abstractmethod
    def upload_file(
        self,
        destination_path: str,
        content: BytesIO,
    ) -> None:
        pass

    @abstractmethod
    def download_file(self, path: str) -> BytesIO:
        pass

    @abstractmethod
    def list_files_with_prefix(self, prefix: str) -> list[str]:
        pass

    @abstractmethod
    def list_files_at(
        self,
        prefix: str,
        suffix: str | None = None,
    ) -> list[str]:
        pass

    @abstractmethod
    def list_subfolders_at(self, prefix: str) -> list[str]:
        pass

    def find_latest_file_in_folder(
        self,
        folder_path: str,
    ) -> str | None:
        """Finds the latest file in a folder. Only checks folders named with a timestamp directly in the given folder (does not check subfolders). Returns None if no files are found."""
        dir_content = self.list_subfolders_at(
            folder_path,
        )

        latest_file = None
        latest_time = None

        for entry_time_str in dir_content:
            try:
                entry_time = pendulum.from_format(
                    entry_time_str,
                    FOLDERNAME_DATETIME_FORMAT,
                )
            except Exception:
                continue

            if latest_time is None or entry_time > latest_time:
                latest_time = entry_time
                latest_file = entry_time_str

        return latest_file
