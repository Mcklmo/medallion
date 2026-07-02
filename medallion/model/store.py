from abc import ABC, abstractmethod
from logging import Logger


from medallion.model.base import DataModel, Reader


class BaseStore[In: DataModel](
    Reader[In],
    ABC,
):
    def __init__(self, logger: Logger) -> None:
        self.logger = logger

    @abstractmethod
    def store_message_data(
        self,
        file_prefix: str,
        row: In,
        destination_folder_path: str,
        cache_path: str | None,
    ) -> None:
        pass
