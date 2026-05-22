from io import BytesIO
from typing import Iterator
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.log import create_logger
from medallion.model.extractor import BaseExtractor
import requests


class DispatchScadaExtractor(BaseExtractor[BytesIO]):
    logger = create_logger()
    max_files_per_run = 10000

    def extract(self) -> Iterator[BytesIO]:
        r = requests.get("https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/")
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.find_all("a")
        files_count = 0
        breakout = False

        for i, link in enumerate(sorted(links, key=lambda x: x["href"], reverse=True)):
            if breakout:
                break

            prepped = requests.Request(
                "GET", f"https://www.nemweb.com.au/{link['href']}"
            ).prepare()
            self.logger.info(f"Extract from url ({i+1}/{len(links)}): {prepped.url}")

            response_file = requests.Session().send(prepped)
            response_file.raise_for_status()

            with ZipFile(BytesIO(response_file.content)) as zip_file:
                extracted_file_names = zip_file.namelist()
                self.logger.info(
                    f"Send {len(extracted_file_names)} csv files from {prepped.url}"
                )

                for j, file in enumerate(extracted_file_names):
                    if files_count > self.max_files_per_run:
                        self.logger.warning(
                            f"Reached max files per run limit of {self.max_files_per_run}, skipping remaining files in the zip"
                        )
                        breakout = True

                        break

                    if file.lower().endswith(".csv"):
                        files_count += 1

                        yield BytesIO(zip_file.read(file))

        assert files_count > 0, "No CSV file found in the ZIP archive."

    def read_bytes(self, data: BytesIO) -> Iterator[BytesIO]:
        return [data]

    @property
    def file_extension(self):
        return "csv"

    def write_output(self, output_data: Iterator[BytesIO]) -> Iterator[BytesIO]:
        return output_data
