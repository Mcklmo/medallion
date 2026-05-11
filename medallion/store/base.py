from abc import ABC, abstractmethod
from io import BytesIO
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
