import time
from typing import Iterator

from .model import Model
from medallion.model.extractor import BasePydanticExtractor


class Extractor(BasePydanticExtractor[Model]):
    def extract(self) -> Iterator[Model]:
        items = [
            Model(name="Alice"),
            Model(name="Bob"),
            Model(name="Charlie"),
        ]
        delay_seconds = 0.5

        for item in items:
            yield item
            time.sleep(delay_seconds)
