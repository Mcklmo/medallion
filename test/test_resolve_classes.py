import os
import sys
import pytest  # type: ignore

from medallion.resolve_classes import EXTRACTOR_TYPE_ASSERTION_MESSAGE  # type: ignore

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "cmd"))

from medallion.medallion import main

PACKAGE_BODY = """from medallion.base import BaseExtractor, BaseTransformer
class FakeExtractor(BaseExtractor[list[dict]]):
    def extract(self) -> list[dict]:
        return []

class FakeTransformer(BaseTransformer[list[dict], list[dict]]):
    def transform(self, data: list[dict]) -> list[dict]:
        return data
"""


@pytest.fixture(autouse=True)
def _isolate_import_state(monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    saved = set(sys.modules)
    yield
    for key in list(sys.modules):
        if key not in saved:
            del sys.modules[key]


@pytest.fixture
def user_package(tmp_path, monkeypatch):
    pkg = tmp_path / "fakepkg"
    pkg.mkdir()
    (pkg / "__init__.py").write_text(PACKAGE_BODY)
    monkeypatch.setenv("MEDALLION_ROOT", str(pkg))
    return pkg


def test_happy_path(user_package, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeExtractor", "FakeTransformer"])
    main()
    out = capsys.readouterr().out
    assert "FakeExtractor" in out
    assert "FakeTransformer" in out


def test_missing_class(user_package, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "Nonexistent"])
    with pytest.raises(AssertionError, match="Nonexistent not found"):
        main()


def test_missing_init(tmp_path, monkeypatch):
    monkeypatch.setenv("MEDALLION_ROOT", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["medallion", "Whatever"])
    with pytest.raises(AssertionError, match="No __init__.py"):
        main()


def test_no_args(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion"])
    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 2


def test_transformer_only_should_fail(user_package, monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeTransformer"])
    with pytest.raises(AssertionError, match=EXTRACTOR_TYPE_ASSERTION_MESSAGE):
        main()


PACKAGE_BODY_TYPE_MISMATCH = PACKAGE_BODY.replace(
    "class FakeTransformer(BaseTransformer[list[dict], list[dict]]):",
    "class FakeTransformer(BaseTransformer[int, list[dict]]):",
)


def test_transformer_input_type_mismatch(user_package, monkeypatch):
    (user_package / "__init__.py").write_text(PACKAGE_BODY_TYPE_MISMATCH)
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeExtractor", "FakeTransformer"])
    with pytest.raises(
        AssertionError,
        match=r"\s*Transformer FakeTransformer expects input of type <class 'int'>,\s* but previous output is of type list\[dict\]",
    ):
        main()
