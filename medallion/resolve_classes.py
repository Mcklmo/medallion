import argparse
import importlib
from logging import Logger
import os
import sys
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer, BaseTransformer
from medallion.store.base import get_env_or_default, must_get_env
from pydantic import BaseModel

from medallion.store.base import get_medallion_root
from medallion.store.base import MEDALLION_ROOT_ENV


def resolve_class(
    package_name: str,
    class_name: str,
) -> type:
    try:
        pkg = importlib.import_module(package_name)
    except ModuleNotFoundError as e:
        raise ImportError(f"Package '{package_name}' not found") from e

    cls = getattr(pkg, class_name, None)

    if cls is None:
        raise ImportError(f"{class_name} not found in module {pkg.__path__}")

    return cls


VSCODE_COMMAND = "vscode"
START_PROJECT_COMMAND = "start"


class VscodeOptions(BaseModel):
    include_all: bool = False  # --all


class UserInput(BaseModel):
    class_names: list[str] = []
    vscode: VscodeOptions | None = None  # None => run pipeline; set => configure vscode
    new_project_name: str | None = None  # None => run pipeline; set => start project


def get_user_input() -> UserInput:
    argv = sys.argv[1:]

    if argv and argv[0] == VSCODE_COMMAND:
        vscode_options = parse_vscode_arguments(argv)

        return UserInput(vscode=vscode_options)

    if argv and argv[0] == START_PROJECT_COMMAND:
        if len(argv) < 2 or len(argv) > 2:
            raise ValueError(
                f"Invalid usage of '{START_PROJECT_COMMAND}' command. "
                "Usage: 'medallion start <project_name>'"
            )

        return UserInput(
            new_project_name=argv[1],
        )

    parser = argparse.ArgumentParser(
        prog="medallion",
        description=(
            "Run a medallion scraper pipeline of user-defined classes, "
            f"or use the '{VSCODE_COMMAND}' subcommand to generate VS Code "
            "launch configurations (see 'medallion vscode -h')."
        ),
    )
    parser.add_argument(
        "class_names",
        nargs="+",
        help="Pipeline classes in order: Extractor first, then Transformers.",
    )
    args = parser.parse_args(argv)

    return UserInput(class_names=args.class_names)


def parse_vscode_arguments(argv: list[str]) -> VscodeOptions:
    parser = argparse.ArgumentParser(
        prog=f"medallion {VSCODE_COMMAND}",
        description="Generate VS Code launch.json debug configurations.",
    )
    parser.add_argument(
        "--all",
        dest="include_all",
        action="store_true",
        help="Include all launch configurations.",
    )

    args = parser.parse_args(argv[1:])  # parse args AFTER the 'vscode' token
    vscode_options = VscodeOptions(
        include_all=args.include_all,
    )

    return vscode_options


def resolve_user_package(logger: Logger) -> str:
    MEDALLION_ROOT = get_medallion_root() + "/src"
    root = os.path.abspath(MEDALLION_ROOT)
    # init_file = os.path.join(root, "__init__.py")

    # assert os.path.isfile(init_file), f"No __init__.py found in {root}"

    log_init_once(logger, MEDALLION_ROOT)

    # Walk up while each ancestor is also a package, so the package is imported
    # under its outermost canonical dotted name. Otherwise the user's own
    # absolute import (e.g. `from example.nemweb.model import X`) and the
    # framework's leaf-name import (`nemweb.model`) would load the same file
    # twice under different `sys.modules` keys, producing two distinct class
    # objects that fail identity-based equality.
    parts: list[str] = []
    current = root

    while True:
        parent, name = os.path.split(current)
        parts.insert(0, name)

        if not os.path.isfile(os.path.join(parent, "__init__.py")):
            break

        current = parent

    sys_path_entry = parent
    if sys_path_entry not in sys.path:
        sys.path.insert(0, sys_path_entry)

    package_dotted_name = ".".join(parts)

    try:
        _ = importlib.import_module(package_dotted_name)
    except ModuleNotFoundError as e:
        raise ImportError(f"Package '{package_dotted_name}' not found") from e

    return package_dotted_name


call_count = 0


def log_init_once(logger, MEDALLION_ROOT):
    global call_count
    call_count += 1

    if call_count == 1:
        logger.info(f"Set {MEDALLION_ROOT_ENV} to {MEDALLION_ROOT}")


def load_classes(
    class_names: list[str],
    logger: Logger,
) -> list[type]:
    classes = resolve_classes_from_names(
        class_names,
        logger,
    )
    return classes


def resolve_classes_from_names(
    class_names: list[str],
    logger: Logger,
) -> list[type]:
    package_name = resolve_user_package(logger)
    logger.info(f"Resolving classes from package '{package_name}'")

    classes = [
        resolve_class(
            package_name,
            n,
        )
        for n in class_names
    ]

    return classes


EXTRACTOR_CLASS_ENV_VAR = "EXTRACTOR_CLASS"


def load_extractor_from_env(
    logger: Logger,
) -> BaseExtractor:
    extractor_name = must_get_env(EXTRACTOR_CLASS_ENV_VAR)
    extractor = build_processor_from_name(
        extractor_name,
        logger,
    )

    assert isinstance(
        extractor,
        BaseExtractor,
    ), f"Extractor must be a {BaseExtractor.__name__}, got {type(extractor).__name__}"

    return extractor


TRANSFORMER_CLASS_ENV_VAR = "TRANSFORMER_CLASS"


def load_transformer_from_env(
    logger: Logger,
) -> BaseTransformer | BaseStreamingTransformer:
    transformer_name = must_get_env(TRANSFORMER_CLASS_ENV_VAR)
    transformer = build_processor_from_name(
        transformer_name,
        logger,
    )
    expected_types = (BaseTransformer, BaseStreamingTransformer)
    assert isinstance(
        transformer,
        expected_types,
    ), f"Transformer must be one of {expected_types}, got {type(transformer).__name__}"

    return transformer


def build_processor_from_name(
    processor_name: str,
    logger: Logger,
) -> type:
    _processors = resolve_classes_from_names(
        [processor_name],
        logger,
    )

    assert (
        len(_processors) == 1
    ), f"Expected exactly one processor, got {len(_processors)}"

    return _processors[0](logger)
