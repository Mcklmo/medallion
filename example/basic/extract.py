from example.basic.model import Model
from medallion.base import BasePydanticExtractor


class Extractor(BasePydanticExtractor[Model]):
    def extract(self) -> list[Model]:
        return [
            Model(name="Alice"),
            Model(name="Bob"),
        ]
