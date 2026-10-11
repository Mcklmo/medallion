import logging
from io import BytesIO

from medallion.store.local import LocalStorage


def test_upload_leaves_only_the_file_and_listing_skips_files_mid_write(tmp_path):
    store = LocalStorage(str(tmp_path), logging.getLogger("test-local-storage"))
    store.upload_file("parts/000000.json", BytesIO(b"[1]"))
    (tmp_path / "parts" / ".000001.json.inflight").write_bytes(b"")

    assert store.list_files_at("parts") == ["parts/000000.json"]
    assert store.download_file("parts/000000.json").getvalue() == b"[1]"
    assert sorted(p.name for p in (tmp_path / "parts").iterdir()) == [
        ".000001.json.inflight",
        "000000.json",
    ]
