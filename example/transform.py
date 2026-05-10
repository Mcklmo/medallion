from medallion.base import BaseTransformer


class Transformer(BaseTransformer[list[dict], list[dict]]):
    def transform(self, data: list[dict]) -> list[dict]:
        return [{"name": d["name"].upper()} for d in data]
