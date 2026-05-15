from time import time
from typing import Iterator

from example.basic.model import Model
from medallion.base import BasePydanticExtractor


class Extractor(BasePydanticExtractor[Model]):
    def extract(self) -> Iterator[Model]:
        items = [
            Model(name="Alice"),
            Model(name="Bob"),
            Model(name="Hermann"),
            Model(name="Charlie"),
        ]
        delay_seconds = 0.1

        for item in items:
            yield item
            time.sleep(delay_seconds)
