from medallion.model.transformer import BaseTransformer
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import BaseStreamingTransformer
from medallion.resolve_classes import load_extractor_from_env


def main():
    processor = load_extractor_from_env()
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
