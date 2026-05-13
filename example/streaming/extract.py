from io import BytesIO, TextIOWrapper
import json
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.base import BaseExtractor
import requests
import csv

SCADA_QUEUE_NAME = "nemweb-dispatch-scada-source"


class Extractor(BaseExtractor[dict]):
    @property
    def queue_to(self):
        return SCADA_QUEUE_NAME

    def extract(self) -> dict:
        source_file = self.get_source_file()
        return self.read_bytes(source_file)

    def read_bytes(self, data: BytesIO) -> list[dict]:
        decoded_bytes = data.read()
        reader = csv.DictReader(decoded_bytes.decode().splitlines())
        return list(reader)

    @property
    def file_extension(self):
        return "csv"

    def write_output(self, output_data: list[dict]) -> BytesIO:
        if not output_data:
            return BytesIO()

        output_stream = BytesIO()
        text = TextIOWrapper(
            output_stream,
            encoding="utf-8",
            newline="",
            write_through=True,
        )
        writer = csv.DictWriter(text, fieldnames=output_data[0].keys())

        writer.writeheader()
        writer.writerows(output_data)
        text.flush()
        text.detach()
        output_stream.seek(0)

        return output_stream

    def get_source_file(self):
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
                    return BytesIO(zip_file.read(file))
