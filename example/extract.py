from io import BytesIO
import json

from medallion.base import BaseJSONExtractor


class Extractor(BaseJSONExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return [{"name": "Alice"}, {"name": "Bob"}]

    def read_bytes(self, data: BytesIO) -> list[dict]:
        data.seek(0)
        return json.loads(data.read().decode())
