from medallion.base import BaseJSONExtractor


class Extractor(BaseJSONExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return [{"name": "Alice"}, {"name": "Bob"}]
