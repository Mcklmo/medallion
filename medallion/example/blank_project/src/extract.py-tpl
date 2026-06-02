from typing import Iterable
from medallion.model.extractor import BaseFileExtractor

from medallion.model.extractor import FileOutput


class SourceDataExtractor(BaseFileExtractor):
    def extract(self) -> Iterable[FileOutput]:
        return [
            FileOutput(
                content=b"value\nHello World",
            )
        ]
