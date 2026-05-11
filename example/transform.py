from medallion.base import BaseJSONTransformer


class Transformer(BaseJSONTransformer[list[dict], list[dict]]):
    def transform(
        self,
        data: list[dict],
    ) -> list[dict]:
        return [{"name": d["name"].upper()} for d in data]
