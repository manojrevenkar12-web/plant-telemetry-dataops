"""OpenLineage run events for every pipeline job.

Each run emits START and COMPLETE (or FAIL) with its input and output datasets, their schemas,
row counts and data-quality assertions. Locally the events go to a JSONL file; set
OPENLINEAGE_URL to send them to Marquez, or to an endpoint that forwards to Microsoft Purview.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.facet_v2 import (
    data_quality_assertions_dataset,
    error_message_run,
    output_statistics_output_dataset,
    schema_dataset,
)
from openlineage.client.transport.file import FileConfig, FileTransport

from ..lake.tables import SCHEMAS

NAMESPACE = "plant-telemetry"
PRODUCER = "https://github.com/manojrevenkar12-web/plant-telemetry-dataops"


def _client(log_path: Path) -> OpenLineageClient:
    if url := os.environ.get("OPENLINEAGE_URL"):
        return OpenLineageClient(url=url)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return OpenLineageClient(transport=FileTransport(FileConfig(log_file_path=str(log_path), append=True)))


def _schema_facet(name: str) -> schema_dataset.SchemaDatasetFacet | None:
    s: pa.Schema | None = SCHEMAS.get(name)
    if s is None:
        return None
    return schema_dataset.SchemaDatasetFacet(fields=[schema_dataset.SchemaDatasetFacetFields(name=f.name, type=str(f.type)) for f in s])


class LineageRun:
    def __init__(self, job: str, inputs: list[str], outputs: list[str]):
        self.job, self.inputs, self.outputs = job, inputs, outputs
        self.run_id = str(uuid.uuid4())
        self.row_counts: dict[str, int] = {}
        self.assertions: dict[str, list[data_quality_assertions_dataset.Assertion]] = {}

    def rows(self, dataset: str, n: int) -> None:
        self.row_counts[dataset] = self.row_counts.get(dataset, 0) + n

    def assertion(self, dataset: str, name: str, success: bool, column: str | None = None) -> None:
        self.assertions.setdefault(dataset, []).append(data_quality_assertions_dataset.Assertion(assertion=name, success=success, column=column))

    def _datasets(self):
        def ns(name: str) -> str:
            return "kafka://plant" if name.startswith("plant.") else "delta://lake"

        ins = [InputDataset(namespace=ns(n), name=n, facets={k: v for k, v in {"schema": _schema_facet(n)}.items() if v}) for n in self.inputs]
        outs = []
        for n in self.outputs:
            facets = {}
            if sf := _schema_facet(n):
                facets["schema"] = sf
            if n in self.assertions:
                facets["dataQualityAssertions"] = data_quality_assertions_dataset.DataQualityAssertionsDatasetFacet(assertions=self.assertions[n])
            out_facets = {"outputStatistics": output_statistics_output_dataset.OutputStatisticsOutputDatasetFacet(rowCount=self.row_counts[n])} if n in self.row_counts else {}
            outs.append(OutputDataset(namespace=ns(n), name=n, facets=facets, outputFacets=out_facets))
        return ins, outs

    def event(self, state: RunState, error: str | None = None) -> RunEvent:
        ins, outs = self._datasets()
        run_facets = {"errorMessage": error_message_run.ErrorMessageRunFacet(message=error, programmingLanguage="python")} if error else {}
        return RunEvent(
            eventType=state, eventTime=datetime.now(UTC).isoformat(), run=Run(runId=self.run_id, facets=run_facets),
            job=Job(namespace=NAMESPACE, name=self.job), inputs=ins, outputs=outs, producer=PRODUCER,
        )


@contextmanager
def lineage(job: str, inputs: list[str], outputs: list[str], log_path: Path = Path("lineage/events.jsonl")):
    client = _client(log_path)
    run = LineageRun(job, inputs, outputs)
    client.emit(run.event(RunState.START))
    try:
        yield run
    except Exception as exc:
        client.emit(run.event(RunState.FAIL, error=f"{type(exc).__name__}: {exc}"))
        raise
    client.emit(run.event(RunState.COMPLETE))
