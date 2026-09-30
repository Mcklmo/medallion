import logging
from typing import Iterable

from medallion.model.base import BasePydanticProcessingStep, DataModel, PydanticReader
from medallion.model.extractor import BaseExtractor
from medallion.model.transformer import (
    BasePydanticGatherTransformer,
    BasePydanticStreamingTransformer,
)
from medallion.pipeline import PipeLine
from medallion.queue.mock import MockQueue
from medallion.store.local import LocalStorage

_LOGGER = logging.getLogger("test-batch-and-gather")


class Doc(DataModel):
    id: str
    paragraphs: tuple[str, ...]


class Group(DataModel):
    doc_id: str
    paragraphs: tuple[str, ...]


DOCS = [Doc(id=d, paragraphs=tuple(f"{d}{i}" for i in range(5))) for d in "ab"]


class Docs(BaseExtractor[Doc], BasePydanticProcessingStep[Doc]):
    def extract(self) -> Iterable[Doc]:
        return DOCS


class Split(
    BasePydanticStreamingTransformer[Doc, Group],
    PydanticReader[Doc],
):
    fan_out = True

    def transform_one(self, data: Doc) -> list[Group]:
        return [Group(doc_id=data.id, paragraphs=(p,)) for p in data.paragraphs]


class Upper(
    BasePydanticStreamingTransformer[Group, Group],
    PydanticReader[Group],
):
    batch_size = 4
    max_batch_wait = 0.2
    batch_sizes: list[int] = []

    def transform_one(self, data: Group) -> Group:
        return Group(
            doc_id=data.doc_id,
            paragraphs=tuple(p.upper() for p in data.paragraphs),
        )

    def transform_many(self, items: list[Group]) -> list[Group]:
        Upper.batch_sizes.append(len(items))
        return [self.transform_one(i) for i in items]


class Join(
    BasePydanticGatherTransformer[Group, Doc],
    PydanticReader[Group],
):
    def gather(self, parts: list[Group]) -> Doc:
        return Doc(
            id=parts[0].doc_id,
            paragraphs=tuple(p for g in parts for p in g.paragraphs),
        )


def run_pipeline(store: LocalStorage) -> None:
    PipeLine(
        extractor=Docs(_LOGGER),
        transformers=[Split(_LOGGER), Upper(_LOGGER), Join(_LOGGER)],
        queues=[MockQueue() for _ in range(4)],
        logger=_LOGGER,
        store_output=store,
        force_run_transformer=False,
    ).run()


def joined_outputs(store: LocalStorage) -> list[list[Doc]]:
    files = store.list_files_at("Docs/Split/Upper/Join", suffix="data.json")
    return [
        sorted(Join(_LOGGER).load_cached(store.download_file(f)), key=lambda d: d.id)
        for f in files
    ]


def test_fan_out_batch_and_gather(tmp_path, monkeypatch):
    monkeypatch.setenv("FORCE_RUN_EXTRACTOR", "false")
    store = LocalStorage(str(tmp_path), _LOGGER)
    expected = [
        Doc(id=d.id, paragraphs=tuple(p.upper() for p in d.paragraphs)) for d in DOCS
    ]
    Upper.batch_sizes = []

    run_pipeline(store)

    assert joined_outputs(store) == [expected]
    assert sum(Upper.batch_sizes) == 10
    assert max(Upper.batch_sizes) > 1

    Upper.batch_sizes = []

    run_pipeline(store)

    assert Upper.batch_sizes == []
    assert joined_outputs(store) == [expected, expected]


def test_config_accepts_gather_step():
    from types import SimpleNamespace

    from medallion.configure_entrypoint.pipeline_graph_model import (
        PipelineGraph,
        Transformer,
    )

    # a namespace, not a PipelineGraph: its model_post_init imports the classes from a project package
    graph = SimpleNamespace(
        transformers=[
            Transformer(name="join", class_="Join", reads_from="groups", writes_to="docs")
        ]
    )

    PipelineGraph.validate_transformer_schemas(
        graph,  # type: ignore[arg-type]
        {"groups": Group, "docs": Doc},
        [Join],
    )
