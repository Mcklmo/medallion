from io import BytesIO
from typing import Generator, Iterator
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.log import create_logger
from medallion.model.extractor import BaseExtractor
import requests

from medallion.model.extractor import FileOutput


class DispatchScadaExtractor(BaseExtractor[FileOutput]):
    logger = create_logger()
    max_files_per_run = 1

    def extract(self) -> Iterator[FileOutput]:
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
            self.logger.info(
                f"Extract from url ({i+1}/{len(links)} or {self.max_files_per_run} files): {prepped.url}"
            )

            response_file = requests.Session().send(prepped)
            response_file.raise_for_status()

            with ZipFile(BytesIO(response_file.content)) as zip_file:
                extracted_file_names = zip_file.namelist()

                for j, file in enumerate(extracted_file_names):
                    if breakout:
                        break

                    if file.lower().endswith(".csv"):
                        files_count += 1
                        breakout = files_count >= self.max_files_per_run

                        self.logger.info(
                            f"Extracting file ({j+1}/{len(extracted_file_names)}): {file}"
                        )

                        yield FileOutput(
                            content=zip_file.read(file),
                        )

        assert files_count > 0, "No CSV file found in the ZIP archive."

    def read_bytes(self, data: BytesIO) -> FileOutput:
        extracted_bytes = data.getvalue()
        return FileOutput.model_validate_json(extracted_bytes)

    @property
    def file_extension(self):
        return "csv"

    def write_output(
        self,
        output_data: Iterator[FileOutput] | FileOutput,
    ) -> Iterator[BytesIO]:
        if isinstance(output_data, list) or isinstance(output_data, Generator):
            return [BytesIO(d.model_dump_json().encode()) for d in output_data]

        return BytesIO(output_data.model_dump_json().encode())
