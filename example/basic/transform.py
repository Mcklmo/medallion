from time import time

from example.basic.model import Model
from medallion.base import BasePydanticStreamingTransformer


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
