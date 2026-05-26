import importlib
import logging
import os
import sys
from io import StringIO
from pydantic import ValidationError
import pytest
from medallion.model.extractor import BaseExtractor
from medallion.resolve_classes import resolve_classes_from_names, resolve_user_package

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "cmd"))

from medallion.medallion import main


def _make_capture_logger() -> tuple[logging.Logger, StringIO]:
    buf = StringIO()
    logger = logging.getLogger(f"test-capture-{id(buf)}")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(buf)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger, buf


_NULL_LOGGER = logging.getLogger("test-null")

MESSAGE_QUEUE_NAME_1 = "queue1"

PACKAGE_BODY = f"""
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseTransformer
from io import BytesIO
from pydantic import BaseModel

class Model(BaseModel):
    name: str

class MockProcessingStep:
    @property
    def file_extension(self) -> str:
        return "txt"

    def write_output(self, output_data) -> BytesIO:
        return BytesIO(b"")

class FakeExtractor(MockProcessingStep, BaseExtractor[Model]):
    def extract(self) -> Model:
        return Model(name="")

    def read_bytes(self, data: BytesIO) -> Model:
        return Model(name="")

    def queue_to(self) -> str:
        return f"{MESSAGE_QUEUE_NAME_1}"

class FakeTransformer(MockProcessingStep, BaseTransformer[Model, Model]):
    def transform(self, data: Model) -> Model:
        return data

    def read_bytes(self, data: BytesIO) -> Model:
        return Model(name="")

    def queue_from(self) -> str:
        return f"{MESSAGE_QUEUE_NAME_1}"

    def queue_to(self) -> str:
        return "some_other_queue"
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


@pytest.fixture
def local_storage_env(tmp_path, monkeypatch):
    output_dir = tmp_path / "output"
    cache_dir = tmp_path / "cache"
    output_dir.mkdir()
    cache_dir.mkdir()
    monkeypatch.setenv("FILE_STORAGE_TYPE", "local")
    monkeypatch.setenv("LOCAL_OUTPUT_DIR", str(output_dir))
    monkeypatch.setenv("LOCAL_CACHE_DIR", str(cache_dir))


def test_happy_path(user_package, local_storage_env, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeExtractor", "FakeTransformer"])
    logger, buf = _make_capture_logger()
    main(logger)
    out = buf.getvalue()
    assert "FakeExtractor" in out
    assert "FakeTransformer" in out


def test_missing_class(user_package, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "Nonexistent"])
    with pytest.raises(AssertionError, match="Nonexistent not found"):
        main(_NULL_LOGGER)


def test_missing_init(tmp_path, monkeypatch):
    monkeypatch.setenv("MEDALLION_ROOT", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["medallion", "Whatever"])
    with pytest.raises(AssertionError, match="No __init__.py"):
        main(_NULL_LOGGER)


def test_no_args(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion"])
    with pytest.raises(SystemExit) as excinfo:
        main(_NULL_LOGGER)
    assert excinfo.value.code == 2


def test_transformer_only_should_fail(user_package, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["medallion", "FakeTransformer"])
    with pytest.raises(
        ValidationError,
        match=f"Input should be an instance of {BaseExtractor.__name__}",
    ):
        main(_NULL_LOGGER)


PACKAGE_BODY_TYPE_MISMATCH = PACKAGE_BODY.replace(
    "BaseTransformer[Model, Model]",
    "BaseTransformer[int, Model]",
)


def test_transformer_input_type_mismatch(user_package, monkeypatch):
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
    inner.mkdir()
    (inner / "__init__.py").write_text(
        "from pydantic import BaseModel\n"
        "class Thing(BaseModel):\n"
        "    name: str = ''\n"
    )

    monkeypatch.setenv("MEDALLION_ROOT", str(inner))

    assert resolve_user_package(_NULL_LOGGER) == "outer.inner"

    # The class resolved via the framework must be identical to the one
    # obtained through a normal absolute import — same `sys.modules` entry.
    [framework_cls] = resolve_classes_from_names(["Thing"], _NULL_LOGGER)
    direct_cls = importlib.import_module("outer.inner").Thing
    assert framework_cls is direct_cls
