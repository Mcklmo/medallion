from example.basic.model import Model
from medallion.base import BasePydanticTransformer


class Transformer(
    BasePydanticTransformer[
        Model,
        Model,
    ]
):
    def transform(
        self,
        data: list[Model],
    ) -> list[Model]:
        return [Model(name=d.name.upper()) for d in data]
