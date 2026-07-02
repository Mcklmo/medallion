from logging import Logger
from typing import Any, Literal
from pydantic import BaseModel, ConfigDict, Field

from medallion.log import create_logger
from medallion.model.extractor import BaseExtractor
from medallion.model.store import BaseStore
from medallion.model.transformer import BaseStreamingTransformer, BaseTransformer
from medallion.resolve_classes import resolve_classes_from_names


class StrictModel(BaseModel):
    """Base with extra='forbid' so typos in YAML keys raise errors."""

    model_config = ConfigDict(extra="forbid")


class Repo(StrictModel):
    name: str
    owner: str | None = None
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


class EffectiveRuntime(StrictModel):
    cpu: str
    memory: str
    timeout: str
    min_instances: int
    max_instances: int
    concurrency: int


_RUNTIME_FALLBACKS = EffectiveRuntime(
    cpu="1",
    memory="512Mi",
    timeout="300s",
    min_instances=1,
    max_instances=10,
    concurrency=10,
)


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
    timezone: str = "UTC"


MAX_PROCESSOR_NAME_LENGTH = 50


class ProcessorBase(StrictModel):
    """Common fields for extractors, transformers, and stores."""

    name: str = Field(
        max_length=MAX_PROCESSOR_NAME_LENGTH,  # gcp service name have 50 character limit
    )
    class_: str = Field(alias="class")
    runtime: Runtime | None = None

    model_config = ConfigDict(
        extra="forbid",
        populate_by_name=True,
    )

    def model_post_init(self, context: Any) -> None:
        if len(self.name) > MAX_PROCESSOR_NAME_LENGTH:
            raise ValueError(
                f"Processor name '{self.name}' is too long after prefixing; must be at most {MAX_PROCESSOR_NAME_LENGTH} characters including prefix"
            )


class Extractor(ProcessorBase):
    writes_to: str
    schedules: list[Schedule] | None = None

    def model_post_init(self, context: Any) -> None:
        extract_prefix = "extract-"
        if not self.name.lower().startswith(extract_prefix.lower()):
            self.name = f"{extract_prefix}{self.name}"


class Transformer(ProcessorBase):
    reads_from: str
    writes_to: str

    def model_post_init(self, context: Any) -> None:
        transform_prefix = "transform-"
        if not self.name.lower().startswith(transform_prefix.lower()):
            self.name = f"{transform_prefix}{self.name}"


class Store(ProcessorBase):
    reads_from: str

    def model_post_init(self, context: Any) -> None:
        store_prefix = "store-"
        if not self.name.lower().startswith(store_prefix.lower()):
            self.name = f"{store_prefix}{self.name}"

        if self.runtime is not None:
            if (
                self.runtime.max_instances is not None
                and self.runtime.max_instances > 1
            ):
                raise ValueError(
                    f"Store '{self.name}' has max_instances={self.runtime.max_instances}, but stores must have max_instances=1 to avoid cross-instance lost-update race conditions"
                )


class PipelineGraph(StrictModel):
    apiVersion: Literal["medallion/v1"]
    repo: Repo
    defaults: Defaults | None = None
    schemas: list[Schema] = Field(default_factory=list)
    queues: list[Queue] = Field(default_factory=list)
    extractors: list[Extractor] = Field(default_factory=list)
    transformers: list[Transformer] = Field(default_factory=list)
    stores: list[Store] = Field(default_factory=list)

    def model_post_init(self, context: Any) -> None:
        logger = create_logger()
        output_schema_by_queue_name = self.map_schemas_to_classes(logger)

        self.validate_extractors_schemas(
            resolve_classes_from_names(
                [e.class_ for e in self.extractors],
                logger=logger,
            ),
            output_schema_by_queue_name,
        )

        self.validate_transformer_schemas(
            output_schema_by_queue_name,
            resolve_classes_from_names(
                [t.class_ for t in self.transformers],
                logger=logger,
            ),
        )

        self.validate_store_schemas(
            output_schema_by_queue_name,
            resolve_classes_from_names(
                [s.class_ for s in self.stores],
                logger=logger,
            ),
        )

    def map_schemas_to_classes(
        self,
        logger: Logger,
    ) -> dict[
        str,
        type,
    ]:
        schema_names = [s.name for s in self.schemas]
        name_to_schema = dict(
            zip(
                schema_names,
                resolve_classes_from_names(
                    schema_names,
                    logger=logger,
                ),
            )
        )

        output_schema_by_queue_name = {
            q.name: name_to_schema[q.schema_] for q in self.queues
        }

        return output_schema_by_queue_name

    def validate_store_schemas(
        self,
        output_schema_by_queue_name: dict[str, type],
        classes_stores: list[type],
    ) -> None:
        for store_config, store_class in zip(
            self.stores,
            classes_stores,
        ):
            assert issubclass(
                store_class,
                BaseStore,
            ), f"Store class {store_class.__name__} must inherit from {BaseStore.__name__}"

            input_schema = output_schema_by_queue_name[store_config.reads_from]

            assert (
                store_class.input_type == input_schema
            ), f"Store {store_config.name} reads from queue {store_config.reads_from} with schema {input_schema.__name__}, but its input_type is {store_class.input_type.__name__}"

    def validate_extractors_schemas(
        self,
        classes_extractor: list[type],
        output_schema_by_queue_name: dict[str, type],
    ) -> None:
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

    def validate_transformer_schemas(
        self,
        output_schema_by_queue_name: dict[str, type],
        classes_transformer: list[type],
    ) -> None:
        for transformer_class, transformer_config in zip(
            classes_transformer,
            self.transformers,
        ):
            input_schema = output_schema_by_queue_name[transformer_config.reads_from]
            output_schema = output_schema_by_queue_name[transformer_config.writes_to]

            allowed_transformer_inheritance = (
                BaseTransformer,
                BaseStreamingTransformer,
            )
            assert issubclass(
                transformer_class,
                allowed_transformer_inheritance,
            ), f"Transformer class {transformer_class.__name__} must inherit from {allowed_transformer_inheritance}"

            assert (
                transformer_class.input_type == input_schema
            ), f"Transformer {transformer_config.name} reads from queue {transformer_config.reads_from} with schema {input_schema.__name__}, but its input_type is {transformer_class.input_type.__name__}"

            assert (
                transformer_class.output_type == output_schema
            ), f"Transformer {transformer_config.name} writes to queue {transformer_config.writes_to} with schema {output_schema.__name__}, but its output_type is {transformer_class.output_type.__name__}"

    def effective_runtime(
        self,
        processor: ProcessorBase,
    ) -> EffectiveRuntime:
        """Merge fallback defaults, top-level defaults.runtime, and the processor's
        own runtime override into a fully-populated EffectiveRuntime. Lower-priority
        layers fill any field the higher-priority layers left as None."""
        merged: dict[str, Any] = _RUNTIME_FALLBACKS.model_dump()

        if isinstance(processor, Store):
            # Store does an unsynchronised read-modify-write of a shared file; running
            # >1 instance re-introduces a cross-instance lost-update race
            merged["max_instances"] = 1

        for layer in (
            self.defaults.runtime if self.defaults else None,
            processor.runtime,
        ):
            if layer is None:
                continue

            for field, value in layer.model_dump(exclude_none=True).items():
                merged[field] = value

        merged["cpu"] = str(merged["cpu"])

        return EffectiveRuntime(**merged)

    def get_pipeline_names(self) -> list[list[str]]:
        """Returns list of pipelines, where each pipeline is a list of processor class names in execution order."""
        graph: dict[
            str,
            GraphEntry,
        ] = {}

        class GraphEntry(BaseModel):
            class_: str
            writes_to: str | None
            reads_from: str | None

        for extractor in self.extractors:
            graph[extractor.name] = GraphEntry(
                class_=extractor.class_,
                reads_from=None,
                writes_to=extractor.writes_to,
            )

        for transformer in self.transformers:
            graph[transformer.name] = GraphEntry(
                class_=transformer.class_,
                reads_from=transformer.reads_from,
                writes_to=transformer.writes_to,
            )

        for store in self.stores:
            graph[store.name] = GraphEntry(
                class_=store.class_,
                reads_from=store.reads_from,
                writes_to=None,
            )

        # Topologically sort the graph based on reads_from and writes_to
        visited = set()
        sorted_processors: list[str] = []

        def visit(node_name):
            """Depth-first visit for topological sort.
            If a node reads from a queue, visit the node that writes to that queue first.
            Then add the current node to the sorted list.

            This ensures that for any processor, all of its dependencies (processors that write to queues it reads from) come before it in the sorted list.
            """
            if node_name in visited:
                return

            visited.add(node_name)

            node = graph[node_name]
            if node.reads_from is not None:
                # Find the processor that writes to the queue this node reads from
                for other_name, other_node in graph.items():
                    if other_node.writes_to == node.reads_from:
                        visit(other_name)

            sorted_processors.append(node.class_)

        for node_name in graph:
            visit(node_name)

        sorted_pipeline_processors = [[cls for cls in sorted_processors]]

        return sorted_pipeline_processors
