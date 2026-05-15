from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field

from medallion.base import BaseExtractor, BaseTransformer
from medallion.resolve_classes import resolve_classes_from_names


class StrictModel(BaseModel):
    """Base with extra='forbid' so typos in YAML keys raise errors."""

    model_config = ConfigDict(extra="forbid")


class Repo(StrictModel):
    name: str
    owner: str
    description: str | None = None


class Runtime(StrictModel):
    cpu: int | float | None = None
    memory: str | None = Field(
        default=None,
        description='e.g. "512Mi", "1Gi"',
    )
    timeout: str | None = Field(
        default=None,
        description='e.g. "300s", "1800s"',
    )
    min_instances: int | None = Field(
        default=None,
        ge=0,
    )
    max_instances: int | None = Field(
        default=None,
        ge=1,
    )
    concurrency: int | None = Field(
        default=None,
        ge=1,
    )


class Defaults(StrictModel):
    runtime: Runtime | None = None


class Schema(StrictModel):
    name: str


class Queue(StrictModel):
    name: str
    schema_: str = Field(
        alias="schema"
    )  # 'schema' shadows BaseModel.schema in v1; safe in v2 but aliasing is clearer

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


class Schedule(StrictModel):
    name: str
    cron: str
    timezone: str


class ProcessorBase(StrictModel):
    """Common fields for extractors, transformers, and stores."""

    name: str
    class_: str = Field(alias="class")
    runtime: Runtime | None = None

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )


class Extractor(ProcessorBase):
    writes_to: str
    schedules: list[Schedule] | None = None


class Transformer(ProcessorBase):
    reads_from: str
    writes_to: str


class Store(ProcessorBase):
    reads_from: str


class PipelineGraph(StrictModel):
    apiVersion: Literal["medallion/v1"]
    repo: Repo
    defaults: Defaults | None = None
    schemas: list[Schema] = Field(default_factory=list)
    queues: list[Queue] = Field(default_factory=list)
    extractors: list[Extractor] = Field(default_factory=list)
    transformers: list[Transformer] = Field(default_factory=list)

    def model_post_init(self, context: Any) -> None:
        schema_names = [s.name for s in self.schemas]
        classes_schemas = resolve_classes_from_names(schema_names)
        classes_extractor = resolve_classes_from_names(
            [e.class_ for e in self.extractors]
        )
        classes_transformer = resolve_classes_from_names(
            [t.class_ for t in self.transformers]
        )
        schema_names = [s.name for s in self.schemas]
        name_to_schema = dict(zip(schema_names, classes_schemas))

        output_schema_by_queue_name = {
            q.name: name_to_schema[q.schema_] for q in self.queues
        }

        for extractor_class, extractor_config in zip(
            classes_extractor,
            self.extractors,
        ):
            assert issubclass(
                extractor_class,
                BaseExtractor,
            ), f"Extractor class {extractor_class.__name__} must inherit from {BaseExtractor.__name__}"
            schema = output_schema_by_queue_name[extractor_config.writes_to]

            assert (
                extractor_class.output_type == schema
            ), f"Extractor {extractor_config.name} writes to queue {extractor_config.writes_to} with schema {schema.__name__}, but its output_type is {extractor_class.output_type}"

        for transformer_class, transformer_config in zip(
            classes_transformer,
            self.transformers,
        ):
            input_schema = output_schema_by_queue_name[transformer_config.reads_from]
            output_schema = output_schema_by_queue_name[transformer_config.writes_to]

            assert issubclass(
                transformer_class,
                BaseTransformer,
            ), f"Transformer class {transformer_class.__name__} must inherit from {BaseTransformer.__name__}"

            assert (
                transformer_class.input_type == input_schema
            ), f"Transformer {transformer_config.name} reads from queue {transformer_config.reads_from} with schema {input_schema.__name__}, but its input_type is {transformer_class.input_type.__name__}"

            assert (
                transformer_class.output_type == output_schema
            ), f"Transformer {transformer_config.name} writes to queue {transformer_config.writes_to} with schema {output_schema.__name__}, but its output_type is {transformer_class.output_type.__name__}"
