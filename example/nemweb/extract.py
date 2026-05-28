from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from io import BytesIO
from itertools import islice
from typing import Generator, Iterator
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.model.extractor import BaseExtractor
import requests

from medallion.model.extractor import FileOutput


class DispatchScadaExtractor(BaseExtractor[FileOutput]):
    max_files_per_run = 1
    max_concurrent_downloads = 20
    timeout = 5

    def extract(self) -> Iterator[FileOutput]:
        session = requests.Session()

        prepped = requests.Request(
            "GET",
            "https://www.nemweb.com.au/REPORTS/CURRENT/Dispatch_SCADA/",
        ).prepare()
        self.logger.info(f"Getting file urls from {prepped.url}")

        listing = session.send(
            prepped,
            timeout=self.timeout,
        )
        listing.raise_for_status()

        links = BeautifulSoup(listing.text, "html.parser").find_all("a")
        urls = [
            f"https://www.nemweb.com.au/{link['href']}"
            for link in sorted(links, key=lambda x: x["href"], reverse=True)
        ]

        def _download(url: str) -> requests.Response:
            self.logger.info(f"Downloading {url}...")

            response = session.get(
                url,
                timeout=self.timeout,
            )

            return response

        files_count = 0
        url_iter = iter(urls)
        executor = ThreadPoolExecutor(max_workers=self.max_concurrent_downloads)
        in_flight: dict = {
            executor.submit(_download, u): u
            for u in islice(url_iter, self.max_concurrent_downloads)
        }

        try:
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for fut in done:
                    url = in_flight.pop(fut)
                    response = fut.result()
                    if response.status_code == 403:
                        self.logger.warning(
                            f"Stopping scraping after ({files_count}/{len(urls)} or {self.max_files_per_run}) files due to 403 response: {url}"
                        )
                        return

                    response.raise_for_status()

                    with ZipFile(BytesIO(response.content)) as zip_file:
                        for member in zip_file.namelist():
                            if not member.lower().endswith(".csv"):
                                continue

                            files_count += 1
                            self.logger.info(
                                f"Yielding csv ({files_count}/{len(urls)} or {self.max_files_per_run}) from {url}: {member}"
                            )

                            yield FileOutput(content=zip_file.read(member))

                            if files_count >= self.max_files_per_run:
                                return

                    next_url = next(url_iter, None)
                    if next_url is not None:
                        in_flight[executor.submit(_download, next_url)] = next_url
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

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
