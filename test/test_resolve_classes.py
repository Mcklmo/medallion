import importlib
import logging
import os
import sys
from pydantic import ValidationError
import pytest
from medallion.model.extractor import BaseExtractor
from medallion.resolve_classes import resolve_classes_from_names, resolve_user_package
from medallion.store.base import FILE_STORAGE_TYPE_ENV_VAR
from medallion.store.initialize_storage import (
    FILE_STORAGE_LOCAL,
    LOCAL_OUTPUT_DIR_ENV_VAR,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "cmd"))

from medallion.medallion import main

_NULL_LOGGER = logging.getLogger("test-null")

PACKAGE_BODY = """
from medallion.model.base import DataModel, PydanticReader
from medallion.model.extractor import BasePydanticExtractor
from medallion.model.transformer import BasePydanticStreamingTransformer

class Model(DataModel):
    name: str

class FakeExtractor(BasePydanticExtractor[Model]):
    def extract(self) -> Model:
        return Model(name="")

class FakeTransformer(
    BasePydanticStreamingTransformer[Model, Model], PydanticReader[Model]
):
    def transform_one(self, data: Model) -> Model:
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
    root = tmp_path / "fakepkg"
    pkg = root / "src"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(PACKAGE_BODY)
    monkeypatch.setenv("MEDALLION_ROOT", str(root))
    return pkg


@pytest.fixture
def local_storage_env(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"
    output_dir.mkdir()
    cache_dir.mkdir()
    monkeypatch.setenv(FILE_STORAGE_TYPE_ENV_VAR, FILE_STORAGE_LOCAL)
    monkeypatch.setenv(LOCAL_OUTPUT_DIR_ENV_VAR, str(output_dir))
    monkeypatch.setenv("LOCAL_CACHE_DIR", str(cache_dir))
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "false")
    monkeypatch.setenv("FORCE_RUN_TRANSFORMER", "false")


def test_happy_path(user_package, local_storage_env, tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeExtractor", "FakeTransformer"])
    main(_NULL_LOGGER)
    assert (tmp_path / "output" / "FakeExtractor" / "FakeTransformer").is_dir()


def test_missing_class(user_package, local_storage_env, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "Nonexistent"])
    with pytest.raises(ImportError, match="Nonexistent not found"):
        main(_NULL_LOGGER)


def test_missing_src(tmp_path, local_storage_env, monkeypatch):
    monkeypatch.setenv("MEDALLION_ROOT", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["medallion", "Whatever"])
    with pytest.raises(ImportError, match="Package 'src' not found"):
        main(_NULL_LOGGER)


def test_no_args(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion"])
    with pytest.raises(SystemExit) as excinfo:
        main(_NULL_LOGGER)
    assert excinfo.value.code == 2


def test_transformer_only_should_fail(user_package, local_storage_env, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeTransformer"])
    with pytest.raises(
        ValidationError,
        match=f"Input should be an instance of {BaseExtractor.__name__}",
    ):
        main(_NULL_LOGGER)


PACKAGE_BODY_TYPE_MISMATCH = PACKAGE_BODY.replace(
    "BasePydanticStreamingTransformer[Model, Model]",
    "BasePydanticStreamingTransformer[int, Model]",
)


def test_transformer_input_type_mismatch(user_package, local_storage_env, monkeypatch):
    (user_package / "__init__.py").write_text(PACKAGE_BODY_TYPE_MISMATCH)
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeExtractor", "FakeTransformer"])
    with pytest.raises(
        ValidationError,
        match=r"Transformer FakeTransformer expects input of type <class 'int'>",
    ):
        main(_NULL_LOGGER)


def test_canonical_dotted_name_with_parent_package(tmp_path, monkeypatch):
    """When the user's package sits inside another package (ancestor has its
    own __init__.py), `resolve_user_package` must return the *outermost* dotted
    name. Otherwise the framework loads the module under a shorter name (e.g.
    `inner`) while the user's absolute imports load it as `outer.inner`,
    producing two distinct class identities for the same source file."""
    outer = tmp_path / "outer"
    outer.mkdir()
    (outer / "__init__.py").write_text("")
    inner = outer / "inner"
    (inner / "src").mkdir(parents=True)
    (inner / "__init__.py").write_text("")
    (inner / "src" / "__init__.py").write_text(
        "from pydantic import BaseModel\n"
        "class Thing(BaseModel):\n"
        "    name: str = ''\n"
    )

    monkeypatch.setenv("MEDALLION_ROOT", str(inner))

    assert resolve_user_package(_NULL_LOGGER) == "outer.inner.src"

    # The class resolved via the framework must be identical to the one
    # obtained through a normal absolute import — same `sys.modules` entry.
    [framework_cls] = resolve_classes_from_names(["Thing"], _NULL_LOGGER)
    direct_cls = importlib.import_module("outer.inner.src").Thing
    assert framework_cls is direct_cls
