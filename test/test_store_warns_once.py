import logging

from medallion.model.base import DataModel
from medallion.run.store import PydanticFlatFileStore
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-store-warns-once")


class Item(DataModel):
    id: int


def test_storing_without_a_cache_folder_warns_once_per_run(tmp_path, caplog):
    store = PydanticFlatFileStore(LocalStorage(str(tmp_path), _LOGGER), _LOGGER, Item)

    with caplog.at_level(logging.WARNING, logger=_LOGGER.name):
        for i in range(3):
            store.store_message_data("data", [Item(id=i)], "Items/2026/run-a")
        store.store_message_data("data", [Item(id=9)], "Items/2026/run-b")

    warnings = [r for r in caplog.records if "Skipping cache storage" in r.getMessage()]
    assert [w.getMessage().split(" for ")[1].split(" ")[0] for w in warnings] == [
        "Items/2026/run-a/data.json",
        "Items/2026/run-b/data.json",
    ]
