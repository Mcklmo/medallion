import logging
from io import BytesIO

from medallion.model.base import DataModel
from medallion.model.transformer import BaseStreamingTransformer
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-step-version")


class Item(DataModel):
    text: str


class Upper(BaseStreamingTransformer[Item, Item]):
    def transform_one(self, data: Item) -> Item:
        return Item(text=data.text.upper())

    def write_output(self, output_data: Item) -> BytesIO:
        return BytesIO(output_data.model_dump_json().encode())

    def load_cached(self, data: BytesIO | bytes) -> Item:
        raise NotImplementedError

    def read_input_bytes(self, data: list[bytes] | bytes) -> list[Item]:
        raise NotImplementedError


def test_key_includes_version():
    item = Item(text="a")

    assert item.default_cache_key("Upper", "1") == item.default_cache_key("Upper", "1")
    assert item.default_cache_key("Upper", "1").startswith("cache/Upper/v1/")
    assert item.default_cache_key("Upper", "1") != item.default_cache_key("Upper", "2")
    assert DataModel.cache_list("Upper", [item], "1") != DataModel.cache_list(
        "Upper", [item], "2"
    )


def test_version_bump_turns_hit_into_miss(tmp_path, monkeypatch):
    store = LocalStorage(str(tmp_path), _LOGGER)
    items: list[DataModel] = [Item(text="a")]
    step = Upper(_LOGGER)

    assert step.check_cache(store, items) is None
    store.upload_file(
        f"{DataModel.cache_list(step.name, items, step.version)}/0.json",
        BytesIO(b"[]"),
    )

    assert step.check_cache(store, items) is not None
    monkeypatch.setattr(Upper, "version", "2")
    assert step.check_cache(store, items) is None
