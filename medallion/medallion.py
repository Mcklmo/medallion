from medallion.resolve_classes import load_classes_from_user_input


def main() -> None:
    classes = load_classes_from_user_input()

    print(f"Running pipeline with extractor {classes.extractor.__class__.__name__}")
    if classes.transformers:
        print(
            "and transformers "
            + ", ".join(t.__class__.__name__ for t in classes.transformers)
        )


if __name__ == "__main__":
    main()
