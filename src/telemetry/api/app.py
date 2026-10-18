"""REST (v1) and GraphQL API over the lake: curated telemetry for engineers, apps and AI agents,
pipeline status for operations, and the anomaly model as a scoring endpoint.

Access control: every caller has an API key (stored hashed) with a role and the plants it may
see. Data from other plants is never returned — not in lists, not in GraphQL, not in scores.
Exfiltration controls: row limits per request, a time-window limit on raw data, a rate limit
per key and an audit log line for every data access (who, what, how many rows).
"""

# No `from __future__ import annotations` here: FastAPI needs real annotations to resolve Depends.

import hashlib
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

import polars as pl
import strawberry
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from graphql import GraphQLError
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field
from strawberry.fastapi import GraphQLRouter

from ..edge.devices import device_ids
from ..lake.tables import Lake
from ..ml import anomaly

log = logging.getLogger("telemetry.api")
audit = logging.getLogger("telemetry.audit")

MAX_ROWS = 10_000
MAX_RAW_WINDOW = timedelta(hours=24)
FRESH_S = 300


@dataclass(frozen=True)
class Caller:
    name: str
    role: Literal["reader", "ops"]
    plants: tuple[str, ...]

    def can_see(self, plant: str) -> bool:
        return "*" in self.plants or plant in self.plants

    def visible(self, df: pl.DataFrame, col: str = "plant_id") -> pl.DataFrame:
        return df if "*" in self.plants else df.filter(pl.col(col).is_in(list(self.plants)))


def default_keys_file() -> Path:
    if env := os.environ.get("API_KEYS_FILE"):
        return Path(env)
    checkout = Path(__file__).resolve().parents[3] / "config" / "api_keys.yaml"
    return checkout if checkout.exists() else Path("config/api_keys.yaml")


def load_keys(path: Path, production: bool) -> dict[str, Caller]:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))["keys"]
    if production:
        raw = [k for k in raw if not k.get("demo")]
        if not raw:
            raise RuntimeError("TELEMETRY_ENV=production but only demo API keys are configured")
    return {k["key_sha256"]: Caller(k["name"], k["role"], tuple(k["plants"])) for k in raw}


class RateLimiter:
    """Sliding window per key. Behind Azure API Management this moves to the gateway."""

    def __init__(self, limit: int, window_s: float = 60):
        self.limit, self.window = limit, window_s
        self.hits: dict[str, deque[float]] = defaultdict(deque)

    def check(self, key: str) -> None:
        now = time.monotonic()
        q = self.hits[key]
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.limit:
            raise HTTPException(429, "Rate limit reached for this API key; retry in a minute.")
        q.append(now)


class ScoreBody(BaseModel):
    device_id: str = Field(pattern=r"^[A-Z]{2}\d{2}-L\d-VIB-\d{2}$")
    values: list[float] = Field(min_length=1, max_length=1000)


def _plant(device_id: str) -> str:
    return device_id[:4]


def _rows(df: pl.DataFrame) -> list[dict]:
    return json.loads(df.write_json())


def create_app(
    lake: Lake | None = None,
    registry_dir: Path | None = None,
    lineage_log: Path | None = None,
    keys_file: Path | None = None,
    rate_limit_per_min: int = 600,
    now_fn=None,
) -> FastAPI:
    lake = lake or Lake()
    registry_dir = registry_dir or Path(os.environ.get("MODEL_REGISTRY", "models"))
    lineage_log = lineage_log or Path(os.environ.get("LINEAGE_LOG", "lineage/events.jsonl"))
    keys = load_keys(keys_file or default_keys_file(), production=os.environ.get("TELEMETRY_ENV") == "production")
    limiter = RateLimiter(rate_limit_per_min)
    now_fn = now_fn or (lambda: datetime.now(UTC))

    prom = CollectorRegistry()
    requests_total = Counter("api_requests_total", "API requests", ["route", "status"], registry=prom)
    latency = Histogram("api_request_seconds", "API latency", ["route"], registry=prom)
    rows_served = Counter("api_rows_served_total", "Data rows returned", ["caller"], registry=prom)

    app = FastAPI(title="Plant Telemetry API", version="1.0.0", docs_url="/docs", redoc_url=None)

    @app.middleware("http")
    async def headers_and_metrics(request: Request, call_next):
        t0 = time.perf_counter()
        response = await call_next(request)
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")
        requests_total.labels(path, str(response.status_code)).inc()
        latency.labels(path).observe(time.perf_counter() - t0)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Cache-Control"] = "no-store"
        if not path.startswith(("/docs", "/graphql", "/openapi")):
            response.headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
        return response

    def authenticate(key: str | None) -> Caller:
        if not key:
            raise HTTPException(401, "Missing X-API-Key header")
        caller = keys.get(hashlib.sha256(key.encode()).hexdigest())
        if caller is None:
            raise HTTPException(401, "Unknown API key")
        limiter.check(caller.name)
        return caller

    def caller_dep(x_api_key: Annotated[str | None, Header()] = None) -> Caller:
        return authenticate(x_api_key)

    def ops_dep(caller: Annotated[Caller, Depends(caller_dep)]) -> Caller:
        if caller.role != "ops":
            raise HTTPException(403, "This endpoint needs the ops role")
        return caller

    def served(caller: Caller, what: str, n: int) -> None:
        rows_served.labels(caller.name).inc(n)
        audit.info(json.dumps({"caller": caller.name, "resource": what, "rows": n, "at": now_fn().isoformat()}))

    def require_plant(caller: Caller, plant: str) -> None:
        if not caller.can_see(plant):
            # 404 rather than 403: do not confirm that another plant's device exists
            raise HTTPException(404, "Not found")

    # ------------------------------------------------------------------ queries shared by REST and GraphQL

    def device_status(caller: Caller) -> list[dict]:
        last = (
            lake.read("silver/readings", columns=["device_id", "plant_id", "line_id", "event_time"])
            .group_by(["device_id", "plant_id", "line_id"])
            .agg(pl.col("event_time").max().alias("last_reading"))
        )
        seen = {r["device_id"]: r["last_reading"] for r in last.iter_rows(named=True)}
        now = now_fn()
        out = []
        for d in device_ids():
            if not caller.can_see(_plant(d)):
                continue
            ts = seen.get(d)
            age = (now - ts).total_seconds() if ts else None
            out.append(
                {
                    "device_id": d,
                    "plant_id": _plant(d),
                    "line_id": d[5:7],
                    "kind": d.split("-")[2],
                    "last_reading": ts.isoformat() if ts else None,
                    "staleness_s": round(age) if age is not None else None,
                    "status": "never_seen" if ts is None else ("fresh" if age <= FRESH_S else "stale"),
                }
            )
        return out

    def metrics_1m(
        caller: Caller, device_id: str, metric: str | None, start: datetime | None, end: datetime | None, limit: int, source: str = "stream"
    ) -> list[dict]:
        require_plant(caller, _plant(device_id))
        filters = [("device_id", "=", device_id), ("source", "=", source)]
        if metric:
            filters.append(("metric", "=", metric))
        df = lake.read("gold/metrics_1m", filters=filters)
        if start:
            df = df.filter(pl.col("minute") >= start)
        if end:
            df = df.filter(pl.col("minute") < end)
        df = df.sort("minute").tail(limit).drop("event_date")
        served(caller, f"gold/metrics_1m:{device_id}", df.height)
        return _rows(df)

    def line_health(caller: Caller, plant: str, line: str) -> dict:
        require_plant(caller, plant)
        gold = lake.read("gold/metrics_1m", filters=[("plant_id", "=", plant), ("line_id", "=", line), ("source", "=", "stream")])
        if gold.is_empty():
            raise HTTPException(404, "No data for this line")
        latest = gold["minute"].max()
        window = gold.filter(pl.col("minute") > latest - timedelta(minutes=15))
        by = {
            r["metric"]: r
            for r in window.group_by("metric").agg(pl.col("avg").mean(), pl.col("sum").sum(), pl.col("bad_readings").sum()).iter_rows(named=True)
        }
        good = (by.get("good_count") or {}).get("sum") or 0
        reject = (by.get("reject_count") or {}).get("sum") or 0
        vib = (by.get("vibration_mm_s") or {}).get("avg")
        card = anomaly.load_latest(registry_dir)
        vib_dev = f"{plant}-{line}-VIB-01"
        vib_score = anomaly.score(card, vib_dev, [vib])[0] if card and vib is not None and vib_dev in card["devices"] else None
        served(caller, f"line_health:{plant}-{line}", window.height)
        return {
            "plant_id": plant,
            "line_id": line,
            "window": "last 15 minutes",
            "as_of": latest.isoformat(),
            "line_speed_m_min": round(by["line_speed_m_min"]["avg"], 2) if "line_speed_m_min" in by else None,
            "fryer_temp_c": round(by["fryer_temp_c"]["avg"], 1) if "fryer_temp_c" in by else None,
            "vibration_mm_s": round(vib, 3) if vib is not None else None,
            "vibration_anomaly": vib_score,
            "good_packs": good,
            "reject_rate": round(reject / (good + reject), 4) if good + reject else None,
            "bad_readings": int(sum(r["bad_readings"] for r in by.values())),
        }

    def reconciliation(caller: Caller) -> list[dict]:
        df = caller.visible(lake.read("gold/shift_reconciliation")).sort(["plant_id", "line_id"])
        served(caller, "gold/shift_reconciliation", df.height)
        return _rows(df)

    # ------------------------------------------------------------------ REST

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "lake": lake.root, "tables": {t: lake.version(t) for t in ("silver/readings", "gold/metrics_1m")}}

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(prom), media_type="text/plain; version=0.0.4")

    @app.get("/v1/devices")
    def devices(caller: Annotated[Caller, Depends(caller_dep)]):
        return {"devices": device_status(caller)}

    @app.get("/v1/telemetry/{device_id}")
    def telemetry(
        device_id: str,
        caller: Annotated[Caller, Depends(caller_dep)],
        metric: str | None = None,
        start: Annotated[datetime | None, Query(alias="from")] = None,
        end: Annotated[datetime | None, Query(alias="to")] = None,
        resolution: Literal["raw", "1m"] = "1m",
        source: Literal["stream", "historian"] = "stream",
        limit: Annotated[int, Query(ge=1, le=MAX_ROWS)] = 1000,
    ):
        if resolution == "1m":
            return {"device_id": device_id, "resolution": "1m", "rows": metrics_1m(caller, device_id, metric, start, end, limit, source)}
        require_plant(caller, _plant(device_id))
        if not (start and end) or end - start > MAX_RAW_WINDOW:
            raise HTTPException(422, f"Raw data needs 'from' and 'to' no more than {MAX_RAW_WINDOW} apart")
        df = lake.read("silver/readings", filters=[("event_date", "in", sorted({start.date(), end.date()})), ("device_id", "=", device_id)])
        df = df.filter((pl.col("event_time") >= start) & (pl.col("event_time") < end))
        if metric:
            df = df.filter(pl.col("metric") == metric)
        df = (
            df.sort("event_time")
            .head(limit)
            .select(["event_time", "metric", "value", "unit", "quality", "seq", "is_late", "in_range", "operator_ref"])
        )
        served(caller, f"silver/readings:{device_id}", df.height)
        return {"device_id": device_id, "resolution": "raw", "rows": _rows(df), "truncated": df.height == limit}

    @app.get("/v1/lines/{plant_id}/{line_id}/health")
    def health(plant_id: str, line_id: str, caller: Annotated[Caller, Depends(caller_dep)]):
        return line_health(caller, plant_id, line_id)

    @app.get("/v1/reconciliation")
    def recon(caller: Annotated[Caller, Depends(caller_dep)]):
        return {"shifts": reconciliation(caller)}

    @app.get("/v1/models/vibration-anomaly")
    def model_card(caller: Annotated[Caller, Depends(caller_dep)]):
        card = anomaly.load_latest(registry_dir)
        if not card:
            raise HTTPException(404, "No model trained yet")
        return {**card, "devices": {d: v for d, v in card["devices"].items() if caller.can_see(_plant(d))}}

    @app.post("/v1/models/vibration-anomaly/score")
    def score(body: ScoreBody, caller: Annotated[Caller, Depends(caller_dep)]):
        require_plant(caller, _plant(body.device_id))
        card = anomaly.load_latest(registry_dir)
        if not card:
            raise HTTPException(503, "No model trained yet")
        try:
            results = anomaly.score(card, body.device_id, body.values)
        except KeyError:
            raise HTTPException(404, "The model has no baseline for this device") from None
        served(caller, f"score:{body.device_id}", len(results))
        return {"model": card["name"], "version": card["version"], "results": results}

    @app.get("/v1/pipeline/status")
    def pipeline_status(caller: Annotated[Caller, Depends(ops_dep)]):
        dlq = lake.read("bronze/dead_letters")
        dq = lake.read("ops/dq_results")
        last_run = dq.sort("checked_at").tail(1)["run_id"].to_list()
        latest = dq.filter(pl.col("run_id").is_in(last_run)) if last_run else dq
        tables = [
            "bronze/telemetry_raw",
            "bronze/dead_letters",
            "silver/readings",
            "silver/historian_1m",
            "gold/metrics_1m",
            "gold/shift_reconciliation",
        ]
        return {
            "tables": {t: {"version": lake.version(t)} for t in tables},
            "dead_letters": {r["reason"]: r["len"] for r in dlq.group_by("reason").len().iter_rows(named=True)},
            "data_quality": {
                "run_id": last_run[0] if last_run else None,
                "checks": latest.height,
                "failed": _rows(latest.filter(~pl.col("success")).drop("checked_at")),
            },
            "devices": {s: sum(d["status"] == s for d in device_status(caller)) for s in ("fresh", "stale", "never_seen")},
        }

    @app.get("/v1/lineage")
    def lineage(caller: Annotated[Caller, Depends(ops_dep)], job: str | None = None, limit: Annotated[int, Query(ge=1, le=500)] = 50):
        if not lineage_log.exists():
            return {"events": []}
        events = [json.loads(line) for line in lineage_log.read_text().splitlines() if line.strip()]
        if job:
            events = [e for e in events if e["job"]["name"] == job]
        slim = [
            {
                "job": e["job"]["name"],
                "state": e["eventType"],
                "time": e["eventTime"],
                "run_id": e["run"]["runId"],
                "inputs": [i["name"] for i in e["inputs"]],
                "outputs": [
                    {
                        "name": o["name"],
                        "rows": o.get("outputFacets", {}).get("outputStatistics", {}).get("rowCount"),
                        "assertions": o.get("facets", {}).get("dataQualityAssertions", {}).get("assertions"),
                    }
                    for o in e["outputs"]
                ],
            }
            for e in events[-limit:]
        ]
        return {"events": slim}

    @app.exception_handler(HTTPException)
    async def http_error(_: Request, exc: HTTPException):
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code, headers=exc.headers)

    # ------------------------------------------------------------------ GraphQL (same rules, same queries)

    @strawberry.type
    class Device:
        device_id: str
        plant_id: str
        line_id: str
        kind: str
        status: str
        last_reading: str | None
        staleness_s: int | None

    @strawberry.type
    class Minute:
        minute: str
        metric: str
        n: int
        avg: float | None
        min: float | None
        max: float | None
        bad_readings: int
        late_readings: int

    @strawberry.type
    class AnomalyScore:
        value: float
        score: float
        anomaly: bool

    @strawberry.type
    class LineHealth:
        plant_id: str
        line_id: str
        as_of: str
        line_speed_m_min: float | None
        fryer_temp_c: float | None
        vibration_mm_s: float | None
        vibration_anomaly: AnomalyScore | None
        good_packs: float
        reject_rate: float | None
        bad_readings: int

    @strawberry.type
    class Reconciliation:
        plant_id: str
        line_id: str
        sensor_good_packs: float | None
        mes_good_packs: float
        difference_pct: float | None
        status: str

    def gql_caller(info) -> Caller:
        return info.context["caller"]

    def gql(fn, *args):
        try:
            return fn(*args)
        except HTTPException as exc:
            raise GraphQLError(str(exc.detail), original_error=exc) from None

    @strawberry.type
    class QueryRoot:
        @strawberry.field
        def devices(self, info: strawberry.Info, plant_id: str | None = None) -> list[Device]:
            return [Device(**d) for d in device_status(gql_caller(info)) if plant_id in (None, d["plant_id"])]

        @strawberry.field
        def metrics(self, info: strawberry.Info, device_id: str, metric: str | None = None, limit: int = 120) -> list[Minute]:
            rows = gql(metrics_1m, gql_caller(info), device_id, metric, None, None, min(limit, MAX_ROWS))
            fields = Minute.__annotations__
            return [Minute(**{k: v for k, v in r.items() if k in fields}) for r in rows]

        @strawberry.field
        def line_health(self, info: strawberry.Info, plant_id: str, line_id: str) -> LineHealth:
            h = gql(line_health, gql_caller(info), plant_id, line_id)
            va = h.pop("vibration_anomaly")
            h.pop("window")
            return LineHealth(**h, vibration_anomaly=AnomalyScore(**va) if va else None)

        @strawberry.field
        def reconciliation(self, info: strawberry.Info) -> list[Reconciliation]:
            fields = Reconciliation.__annotations__
            return [Reconciliation(**{k: v for k, v in r.items() if k in fields}) for r in reconciliation(gql_caller(info))]

    async def gql_context(x_api_key: Annotated[str | None, Header()] = None):
        return {"caller": authenticate(x_api_key)}

    class Schema(strawberry.Schema):
        def process_errors(self, errors, execution_context=None):
            # Access-denied / not-found are expected answers, not server errors: keep them out of the error log.
            unexpected = [e for e in errors if e.original_error is not None and not isinstance(e.original_error, (HTTPException, GraphQLError))]
            if unexpected:
                super().process_errors(unexpected, execution_context)

    schema = Schema(query=QueryRoot)
    app.include_router(GraphQLRouter(schema, context_getter=gql_context, graphql_ide=None), prefix="/graphql")
    return app
