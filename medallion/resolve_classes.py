import argparse
import importlib
from logging import Logger
import os
import sys
from typing import Optional

from medallion.base import BaseExtractor, BaseTransformer
from medallion.pipeline import PipeLine
from medallion.store.base import BlobStore


def resolve_class(
    package_name: str,
    class_name: str,
) -> type:
    pkg = importlib.import_module(package_name)
    cls = getattr(pkg, class_name, None)

    assert cls is not None, f"{class_name} not found in {package_name}"

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
    root = os.environ.get("MEDALLION_ROOT") or os.getcwd()
    root = os.path.abspath(root)
    init_file = os.path.join(root, "__init__.py")
    assert os.path.isfile(init_file), f"No __init__.py found in {root}"

    parent, name = os.path.split(root)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    return name


EXTRACTOR_TYPE_ASSERTION_MESSAGE = (
    f"First class must be of type {BaseExtractor.__name__}"
)


def load_classes_from_user_input(
    store_output: BlobStore,
    store_cache: BlobStore,
    logger: Logger,
) -> PipeLine:
    class_names = get_user_specified_class_names()
    package_name = resolve_user_package()
    classes = [resolve_class(package_name, n) for n in class_names]
    extractor = classes[0]()
    transformers = [cls() for cls in classes[1:]] if len(classes) > 1 else None

    validate(
        extractor,
        transformers,
    )

    return PipeLine(
        extractor=extractor,
        transformers=transformers,
        logger=logger,
        store_output=store_output,
        store_cache=store_cache,
    )


def validate(
    extractor: BaseExtractor,
    transformers: Optional[list[BaseTransformer]],
) -> None:
    assert isinstance(
        extractor,
        BaseExtractor,
    ), EXTRACTOR_TYPE_ASSERTION_MESSAGE

    previous_output_type = extractor.output_type

    for t in transformers or []:
        assert isinstance(
            t,
            BaseTransformer,
        ), f"Transformers must be of type {BaseTransformer.__name__}"

        assert t.input_type == previous_output_type, f"""\
            Transformer {t.__class__.__name__} expects input of type {t.input_type}, \
            but previous output is of type {previous_output_type}\
        """
        previous_output_type = t.output_type
