from medallion.model.transformer import BaseJSONTransformer


class Transformer(
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
