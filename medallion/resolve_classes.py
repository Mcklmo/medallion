import argparse
import importlib
from logging import Logger
import os
import sys
from medallion.pipeline import PipeLine
from medallion.store.base import BlobStore


def resolve_class(
    package_name: str,
    class_name: str,
) -> type:
    pkg = importlib.import_module(package_name)
    cls = getattr(pkg, class_name, None)

    assert cls is not None, f"{class_name} not found in {pkg.__path__}"

    return cls


def get_user_specified_class_names() -> list[str]:
    parser = argparse.ArgumentParser(
        prog="medallion",
        description="Run a medallion scraper pipeline of user-defined classes.",
    )
    parser.add_argument(
        "class_names",
        nargs="+",
        help="Pipeline classes in order: Extractor first, then Transformers.",
    )
    return parser.parse_args().class_names


def resolve_user_package() -> str:
    MEDALLION_ROOT = os.getenv("MEDALLION_ROOT") or os.getcwd()
    root = MEDALLION_ROOT
    root = os.path.abspath(root)
    init_file = os.path.join(root, "__init__.py")
    assert os.path.isfile(init_file), f"No __init__.py found in {root}"

    parent, name = os.path.split(root)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    return name


def load_classes(
    store_output: BlobStore,
    store_cache: BlobStore,
    logger: Logger,
    class_names: list[str],
) -> PipeLine:
    classes = resolve_classes_from_names(class_names)
    extractor = classes[0]()
    transformers = [cls() for cls in classes[1:]] if len(classes) > 1 else None

    return PipeLine(
        extractor=extractor,
        transformers=transformers,
        logger=logger,
        store_output=store_output,
        store_cache=store_cache,
    )


def resolve_classes_from_names(class_names: list[str]) -> list[type]:
    package_name = resolve_user_package()
    classes = [
        resolve_class(
            package_name,
            n,
        )
        for n in class_names
    ]

    return classes
