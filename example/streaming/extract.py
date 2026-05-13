from io import BytesIO
import json

from medallion.base import BaseExtractor


class Extractor(BaseExtractor[dict]):
    items = [
        {
            "name": "Alice",
        },
        {
            "name": "Bob",
        },
    ]
    i = 0

    def extract(self) -> dict:
        item = self.items[self.i]
        self.i = (self.i + 1) % len(self.items)

        return item

    def read_bytes(self, data: BytesIO) -> list[dict]:
        data.seek(0)
        return json.loads(data.read().decode())
