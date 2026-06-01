import csv
from medallion.model.extractor import FileOutput
from .model import SourceDataModel
from medallion.model.transformer import BasePydanticStreamingTransformer
from medallion.model.base import FileReader


class SourceDataTransformer(
    BasePydanticStreamingTransformer[
        FileOutput,
        SourceDataModel,
    ],
    FileReader,
):
    def transform_one(
        self,
        data: FileOutput,
    ) -> list[SourceDataModel]:
        decoded_lines = data.content.decode("utf-8").splitlines()
        reader = csv.DictReader(decoded_lines)
        results: list[SourceDataModel] = []

        for row in reader:
            model = SourceDataModel(
                value=row["value"],
            )

            results.append(model)

        return results
