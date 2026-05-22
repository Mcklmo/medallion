from medallion.log import create_logger
from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from medallion.resolve_classes import load_extractor_from_env


def main():
    logger = create_logger()
    processor = load_extractor_from_env(logger)
    supported_processor_classes = (
        BaseExtractor,
        BaseTransformer,
        BaseStreamingTransformer,
    )

    assert isinstance(
        processor,
        supported_processor_classes,
    ), f"Processor must be either {supported_processor_classes}, got {type(processor)}"


if __name__ == "__main__":
    main()
