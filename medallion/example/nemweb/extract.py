from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from io import BytesIO
from itertools import islice
from typing import Iterable
from zipfile import ZipFile
from bs4 import BeautifulSoup
from medallion.model.extractor import BaseFileExtractor
import requests

from medallion.model.extractor import FileOutput


class DispatchScadaExtractor(BaseFileExtractor):
    max_files_per_run = 1
    max_concurrent_downloads = 20
    timeout = 5

    def extract(self) -> Iterable[FileOutput]:
        session = requests.Session()
        urls = self.get_csv_file_links(session)

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

        if files_count == 0:
            raise RuntimeError("No CSV file found in the ZIP archive.")

    def get_csv_file_links(self, session: requests.Session) -> list[str]:
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

        return urls
