"""对外接口：可用性与拒绝原因的可区分性。"""

from __future__ import annotations

import logging

from fastapi.testclient import TestClient

from main import app

log = logging.getLogger("test.api")
client = TestClient(app)


def test_root_still_available():
    r = client.get("/")
    log.info("判定依据: GET / -> %d %s", r.status_code, r.json())
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_reject_reasons_are_distinguishable_over_http():
    bad_res = client.post("/jobs", json={
        "name": "x", "user": "api-user", "request_id": "api-bad-res", "cpu": 0})
    unknown_dep = client.post("/jobs", json={
        "name": "x", "user": "api-user", "request_id": "api-bad-dep",
        "depends_on": ["job-404"]})
    log.info("判定依据: 非法资源 -> %d/%s, 未知依赖 -> %d/%s",
             bad_res.status_code, bad_res.json()["detail"]["reason"],
             unknown_dep.status_code, unknown_dep.json()["detail"]["reason"])
    assert bad_res.status_code == 400
    assert bad_res.json()["detail"]["reason"] == "INVALID_RESOURCES"
    assert unknown_dep.status_code == 404
    assert unknown_dep.json()["detail"]["reason"] == "UNKNOWN_DEPENDENCY"


def test_submit_and_idempotent_resubmit_over_http():
    body = {"name": "x", "user": "api-user", "request_id": "api-idem", "num_tasks": 2}
    r1 = client.post("/jobs", json=body)
    r2 = client.post("/jobs", json=body)
    log.info("判定依据: 首次 created=%s, 重复 created=%s, job_id 一致=%s",
             r1.json()["created"], r2.json()["created"],
             r1.json()["job_id"] == r2.json()["job_id"])
    assert r1.status_code == 201
    assert r1.json()["created"] is True
    assert r2.json()["created"] is False
    assert r1.json()["job_id"] == r2.json()["job_id"]
