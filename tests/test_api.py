import hashlib
import logging
from datetime import UTC, datetime

import pytest
import yaml
from fastapi.testclient import TestClient

from telemetry.api.app import create_app, load_keys
from telemetry.lake.tables import Lake

OPS = {"X-API-Key": "ops-demo-key"}
PL01 = {"X-API-Key": "pl01-demo-key"}
NOW = datetime(2026, 10, 14, 8, 1, tzinfo=UTC)


@pytest.fixture(scope="module")
def client(demo_run):
    out, _ = demo_run
    return TestClient(create_app(Lake(out / "lake"), out / "models", out / "lineage" / "events.jsonl", now_fn=lambda: NOW))


def test_requires_a_valid_key(client):
    assert client.get("/v1/devices").status_code == 401
    assert client.get("/v1/devices", headers={"X-API-Key": "guess"}).status_code == 401
    assert client.get("/healthz").status_code == 200


def test_plant_scoped_key_sees_only_its_plant(client):
    devices = client.get("/v1/devices", headers=PL01).json()["devices"]
    assert devices and {d["plant_id"] for d in devices} == {"PL01"}
    assert len(client.get("/v1/devices", headers=OPS).json()["devices"]) == 45
    assert client.get("/v1/telemetry/DE01-L1-VIB-01", headers=PL01).status_code == 404
    assert client.get("/v1/lines/DE01/L1/health", headers=PL01).status_code == 404
    assert {s["plant_id"] for s in client.get("/v1/reconciliation", headers=PL01).json()["shifts"]} == {"PL01"}
    card = client.get("/v1/models/vibration-anomaly", headers=PL01).json()
    assert all(d.startswith("PL01") for d in card["devices"])


def test_ops_only_endpoints(client):
    assert client.get("/v1/pipeline/status", headers=PL01).status_code == 403
    status = client.get("/v1/pipeline/status", headers=OPS).json()
    assert status["dead_letters"] == {"malformed_json": 5, "contract_v2": 5}
    assert [f["check"] for f in status["data_quality"]["failed"]] == ["completeness(NL01-L1)"]
    assert client.get("/v1/lineage", headers=OPS, params={"job": "quality.checks"}).json()["events"]


def test_limits_against_bulk_extraction(client):
    assert client.get("/v1/telemetry/PL01-L2-VIB-01", headers=PL01, params={"limit": 50_000}).status_code == 422
    raw = client.get("/v1/telemetry/PL01-L2-VIB-01", headers=PL01, params={"resolution": "raw"})
    assert raw.status_code == 422
    ok = client.get(
        "/v1/telemetry/PL01-L2-VIB-01",
        headers=PL01,
        params={"resolution": "raw", "from": "2026-10-14T07:00:00Z", "to": "2026-10-14T07:10:00Z", "limit": 20},
    )
    assert ok.status_code == 200 and len(ok.json()["rows"]) == 20 and ok.json()["truncated"]


def test_line_health_and_scoring_show_the_fault(client):
    h = client.get("/v1/lines/PL01/L2/health", headers=PL01).json()
    assert h["vibration_anomaly"]["anomaly"] is True
    assert client.get("/v1/lines/PL01/L1/health", headers=PL01).json()["vibration_anomaly"]["anomaly"] is False
    s = client.post("/v1/models/vibration-anomaly/score", headers=PL01, json={"device_id": "PL01-L2-VIB-01", "values": [2.5, 8.0]}).json()
    assert [x["anomaly"] for x in s["results"]] == [False, True]
    assert client.post("/v1/models/vibration-anomaly/score", headers=PL01, json={"device_id": "DE01-L2-VIB-01", "values": [1]}).status_code == 404


def test_graphql_applies_the_same_rules(client):
    q = '{ lineHealth(plantId:"PL01", lineId:"L2"){ vibrationAnomaly{ anomaly } } devices{ plantId } }'
    data = client.post("/graphql", headers=PL01, json={"query": q}).json()["data"]
    assert data["lineHealth"]["vibrationAnomaly"]["anomaly"] is True and {d["plantId"] for d in data["devices"]} == {"PL01"}
    denied = client.post("/graphql", headers=PL01, json={"query": '{ metrics(deviceId:"DE01-L1-VIB-01"){ minute } }'}).json()
    assert denied["data"] is None and denied["errors"][0]["message"] == "Not found"
    assert client.post("/graphql", json={"query": "{ devices { deviceId } }"}).status_code == 401


def test_every_data_access_is_audited(client, caplog):
    with caplog.at_level(logging.INFO, logger="telemetry.audit"):
        client.get("/v1/telemetry/PL01-L2-VIB-01", headers=PL01, params={"limit": 5})
    assert any('"caller": "pl01-maintenance"' in r.message and '"rows": 5' in r.message for r in caplog.records)


def test_security_headers(client):
    h = client.get("/healthz").headers
    assert h["x-content-type-options"] == "nosniff" and h["x-frame-options"] == "DENY" and "default-src 'none'" in h["content-security-policy"]


def test_rate_limit(demo_run):
    out, _ = demo_run
    c = TestClient(create_app(Lake(out / "lake"), out / "models", rate_limit_per_min=3))
    codes = [c.get("/v1/models/vibration-anomaly", headers=PL01).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]


def test_production_refuses_demo_keys(tmp_path, monkeypatch):
    f = tmp_path / "keys.yaml"
    f.write_text(
        yaml.safe_dump({"keys": [{"name": "x", "key_sha256": hashlib.sha256(b"k").hexdigest(), "role": "ops", "plants": ["*"], "demo": True}]})
    )
    with pytest.raises(RuntimeError):
        load_keys(f, production=True)
    assert load_keys(f, production=False)
