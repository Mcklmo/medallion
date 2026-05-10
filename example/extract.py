from medallion.base import BaseExtractor


class Extractor(BaseExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return [{"name": "Alice"}, {"name": "Bob"}]
