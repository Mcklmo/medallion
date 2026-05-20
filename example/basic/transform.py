import time

from .model import Model
from medallion.model.transformer import BasePydanticStreamingTransformer


class Transformer(
    BasePydanticStreamingTransformer[
        Model,
        Model,
    ]
):
    def transform_one(
        self,
        data: Model,
    ) -> Model:
        delay_seconds = 0.1
        time.sleep(delay_seconds)

        return Model(name=data.name.upper())
