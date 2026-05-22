from medallion.model.transformer import BaseJSONTransformer


class DispatchScadaTransformer(
    BaseJSONTransformer[
        dict,
        dict,
    ]
):
    def transform(
        self,
        data: dict,
    ) -> dict:
        return {"name": data["name"].upper()}
