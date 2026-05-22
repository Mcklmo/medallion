from io import BytesIO
from typing import Iterator
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.model.extractor import BaseExtractor
import requests


class DispatchScadaExtractor(BaseExtractor[BytesIO]):
    def extract(self) -> Iterator[BytesIO]:
        r = requests.get("https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/")
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a")
        latest = max(
            links,
            key=lambda x: x["href"],
        )["href"]

        response_file = requests.get(f"https://www.nemweb.com.au/{latest}")
        response_file.raise_for_status()

        with ZipFile(BytesIO(response_file.content)) as zip_file:
            for file in zip_file.namelist():
                if file.lower().endswith(".csv"):
                    return [
                        BytesIO(zip_file.read(file)),
                    ]

        raise ValueError("No CSV file found in the ZIP archive.")

    def read_bytes(self, data: BytesIO) -> Iterator[BytesIO]:
        return [data]

    @property
    def file_extension(self):
        return "csv"

    def write_output(self, output_data: Iterator[BytesIO]) -> Iterator[BytesIO]:
        return output_data
