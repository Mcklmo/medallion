from os import getenv
import re
from abc import ABC, abstractmethod
from io import BytesIO
import pendulum

from google.oauth2 import service_account
import google.auth

from dotenv import load_dotenv

load_dotenv()

FOLDERNAME_DATETIME_FORMAT = "YYYY-MM-DDTHH-mm-ssSSS"
FILE_STORAGE_TYPE_ENV_VAR = "FILE_STORAGE_TYPE"
_LATEST_FILE_PATH_REGEX = re.compile(
    r"^(?P<rel>\d{4}/\d{2}/\d{2}/\d{2}/"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\d{3})).*/.+$"
)


class MissingENVError(Exception):
    pass


def must_get_env(key: str) -> str:
    value = getenv(key)
    if value is None:
        raise MissingENVError(f"Required environment variable {key} is missing.")

    return value


def load_service_account_credentials():
    try:
        google_application_credentials_path = must_get_env(
            "GOOGLE_APPLICATION_CREDENTIALS"
        )
    except MissingENVError:
        credentials, _ = google.auth.default()
        return credentials

    return service_account.Credentials.from_service_account_file(
        google_application_credentials_path
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
    def list_files_at(
        self,
        prefix: str,
        suffix: str | None = None,
    ) -> list[str]:
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


MEDALLION_TOPIC_ENV = "MEDALLION_TOPIC"
