"""HTTP 接口测试：既有根路由保持可用，新接口拒绝原因可区分。"""
import logging

import pytest
from fastapi.testclient import TestClient

from main import app
from scheduler.app import create_app
from scheduler.cluster import ClusterState
from scheduler.models import Resources
from scheduler.scheduler import Scheduler

log = logging.getLogger("test.api")
client = TestClient(app)


@pytest.fixture()
def fresh_client():
    """每个用例独立的调度器与集群状态，避免相互影响。"""
    cluster = ClusterState(Resources(cpu=8, mem_mb=16384))
    return TestClient(create_app(Scheduler(cluster), cluster))


def test_root_endpoint_still_works():
    r = client.get("/")
    log.info("输入: GET / | 判定: 200 %s", r.json())
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_submit_and_query_job(fresh_client):
    client = fresh_client
    r = client.post("/jobs", json={
        "owner": "alice", "request_id": "api-1",
        "tasks": [{"name": "t0", "resources": {"cpu": 1, "mem_mb": 512}}],
    })
    log.info("输入: 提交 1x(1CPU,512MB) | 判定: 201 status=%s state=%s",
             r.json()["status"], r.json()["job"]["state"])
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "accepted"
    assert body["job"]["state"] == "running"
    job_id = body["job"]["job_id"]

    r2 = client.get(f"/jobs/{job_id}")
    assert r2.status_code == 200 and r2.json()["job_id"] == job_id

    # 幂等：同 request_id 再提交返回同一作业
    r3 = client.post("/jobs", json={
        "owner": "alice", "request_id": "api-1",
        "tasks": [{"name": "t0", "resources": {"cpu": 1, "mem_mb": 512}}],
    })
    log.info("输入: 同 request_id 重复提交 | 判定: status=%s job_id 相同=%s",
             r3.json()["status"], r3.json()["job"]["job_id"] == job_id)
    assert r3.json()["status"] == "duplicate"
    assert r3.json()["job"]["job_id"] == job_id


def test_reject_reasons_distinguishable_over_http(fresh_client):
    client = fresh_client
    cases = [
        ({"owner": "a", "request_id": "bad1",
          "tasks": [{"name": "t", "resources": {"cpu": 0, "mem_mb": 1}}]},
         "invalid_resources"),
        ({"owner": "a", "request_id": "bad2",
          "tasks": [{"name": "t", "resources": {"cpu": 999, "mem_mb": 1}}]},
         "exceeds_capacity"),
        ({"owner": "a", "request_id": "bad3",
          "tasks": [{"name": "t", "resources": {"cpu": 1, "mem_mb": 1}}],
          "depends_on": ["job-404"]},
         "dependency_not_found"),
    ]
    for payload, expected in cases:
        r = client.post("/jobs", json=payload)
        log.info("输入: %s | 判定: 422 reason=%s == %s",
                 payload["request_id"], r.json()["detail"]["reason"], expected)
        assert r.status_code == 422
        assert r.json()["detail"]["reason"] == expected


def test_complete_and_cancel_flow(fresh_client):
    client = fresh_client
    r = client.post("/jobs", json={
        "owner": "alice", "request_id": "api-flow",
        "tasks": [{"name": f"t{i}", "resources": {"cpu": 1, "mem_mb": 128}}
                  for i in range(2)],
    })
    job_id = r.json()["job"]["job_id"]
    for i in range(2):
        rc = client.post(f"/jobs/{job_id}/complete",
                         json={"task_index": i, "ok": True})
    log.info("输入: 两任务全部完成 | 判定: state=%s", rc.json()["state"])
    assert rc.json()["state"] == "succeeded"

    r2 = client.post("/jobs", json={
        "owner": "alice", "request_id": "api-cancel",
        "tasks": [{"name": "t", "resources": {"cpu": 1, "mem_mb": 128}}],
    })
    jid2 = r2.json()["job"]["job_id"]
    rx = client.post(f"/jobs/{jid2}/cancel")
    log.info("输入: 取消运行中作业 | 判定: state=%s", rx.json()["state"])
    assert rx.json()["state"] == "cancelled"

    unknown = client.post("/jobs/job-999/cancel")
    assert unknown.status_code == 404
