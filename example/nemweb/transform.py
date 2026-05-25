import csv
import decimal
from io import BytesIO
import pendulum

from medallion.model.extractor import FileOutput


from .model import DispatchScadaModel
from medallion.model.transformer import BasePydanticStreamingTransformer


class DispatchScadaTransformer(
    BasePydanticStreamingTransformer[
        FileOutput,
        DispatchScadaModel,
    ]
):
    def transform_one(
        self,
        data: FileOutput,
    ) -> list[DispatchScadaModel]:
        decoded_lines = data.content.decode("utf-8").splitlines()
        reader = csv.reader(decoded_lines)

        results: list[DispatchScadaModel] = []
        header = ""

        for row in reader:
            first_column = row[0]
            if first_column == "I":
                header = row
                continue

            if not header:
                continue

            if first_column != "D":
                continue

            row_dict = dict(zip(header, row))
            model = DispatchScadaModel(
                dispatch=row_dict["DISPATCH"],
                unit_scada=row_dict["UNIT_SCADA"],
                settlementdate=str(pendulum.parse(row_dict["SETTLEMENTDATE"])),
                duid=row_dict["DUID"],
                scadavalue=str(decimal.Decimal(row_dict["SCADAVALUE"])),
                lastchanged=str(pendulum.parse(row_dict["LASTCHANGED"])),
            )

            results.append(model)

        return results

    def read_input_bytes(self, data: BytesIO | bytes) -> FileOutput:
        if isinstance(data, bytes):
            return FileOutput.model_validate_json(data)

        return FileOutput.model_validate_json(data.getvalue())
