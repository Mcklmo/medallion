import re
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

_LATEST_FILE_PATH_REGEX = re.compile(
    r"^(?P<rel>\d{4}/\d{2}/\d{2}/\d{2}/"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\d{3})).*/.+$"
)


def build_timestamp_path_segments(start_time: str) -> list[str]:
    date = pendulum.from_format(
        start_time,
        FOLDERNAME_DATETIME_FORMAT,
    )
    year = date.format("YYYY")
    month = date.format("MM")
    day = date.format("DD")
    hour = date.format("HH")
    timestamp_elements = [
        year,
        month,
        day,
        hour,
    ]

    return timestamp_elements


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
        """Finds the latest run under folder_path. Considers only file paths matching <folder_path>/YYYY/MM/DD/HH/<FOLDERNAME_DATETIME_FORMAT>/... and returns the relative timestamp-folder path (YYYY/MM/DD/HH/<timestamp>) of the latest one. Returns None if no matching file is found."""
        files = self.list_files_at(folder_path)
        prefix = f"{folder_path.rstrip('/')}/"

        latest_rel = None
        latest_time = None

        for file_path in files:
            if not file_path.startswith(prefix):
                continue

            match = _LATEST_FILE_PATH_REGEX.match(file_path[len(prefix) :])
            if not match:
                continue

            try:
                entry_time = pendulum.from_format(
                    match.group("timestamp"),
                    FOLDERNAME_DATETIME_FORMAT,
                )
            except Exception:
                continue

            if latest_time is None or entry_time > latest_time:
                latest_time = entry_time
                latest_rel = match.group("rel")

        return latest_rel
