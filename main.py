"""本地 HPC 作业调度服务：FastAPI 对外接口。

资源模型与调度规则见 README.md；本层只做参数转换与错误映射，
核心一致性逻辑都在 scheduler.engine.Scheduler 内。
"""

from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from scheduler import JobSpec, ResourceRequest, Scheduler
from scheduler.cluster import Cluster, ClusterCapacity
from scheduler.errors import RejectReason, SubmissionError
from scheduler.models import Job

# 本地模拟的集群容量与用户配额（可用环境变量覆盖）
CLUSTER_CPU = int(os.environ.get("CLUSTER_CPU", "16"))
CLUSTER_MEM_MB = int(os.environ.get("CLUSTER_MEM_MB", "16384"))
QUOTA_CPU = int(os.environ.get("QUOTA_CPU", str(CLUSTER_CPU)))
QUOTA_MEM_MB = int(os.environ.get("QUOTA_MEM_MB", str(CLUSTER_MEM_MB)))

scheduler = Scheduler(
    Cluster(ClusterCapacity(total_cpu=CLUSTER_CPU, total_mem_mb=CLUSTER_MEM_MB)),
    quota_cpu=QUOTA_CPU,
    quota_mem_mb=QUOTA_MEM_MB,
)

app = FastAPI(title="local-hpc-scheduler")


class SubmitRequest(BaseModel):
    name: str
    user: str
    request_id: str = Field(description="幂等键：同一 request_id 重复提交只产生一个作业")
    cpu: int = 1
    mem_mb: int = 128
    num_tasks: int = 1
    depends_on: list[str] = []
    max_retries: int = 0


class FailRequest(BaseModel):
    reason: str = "task error"


_REJECT_HTTP = {
    RejectReason.INVALID_RESOURCES: 400,
    RejectReason.UNKNOWN_DEPENDENCY: 404,
    RejectReason.QUOTA_EXCEEDED: 409,
    RejectReason.DUPLICATE_REQUEST: 409,
}


def _job_view(job: Job) -> dict:
    return {
        "job_id": job.job_id,
        "name": job.spec.name,
        "user": job.spec.user,
        "state": job.state.value,
        "num_tasks": job.spec.num_tasks,
        "depends_on": list(job.spec.depends_on),
        "failure_kind": job.failure_kind.value if job.failure_kind else None,
        "failure_reason": job.failure_reason,
        "tasks": [
            {
                "index": t.index,
                "state": t.state.value,
                "attempt": t.attempt,
                "failure_reason": t.failure_reason,
            }
            for t in job.tasks
        ],
    }


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/jobs", status_code=201)
def submit_job(req: SubmitRequest):
    spec = JobSpec(
        name=req.name,
        user=req.user,
        request_id=req.request_id,
        resources=ResourceRequest(cpu=req.cpu, mem_mb=req.mem_mb),
        num_tasks=req.num_tasks,
        depends_on=tuple(req.depends_on),
        max_retries=req.max_retries,
    )
    try:
        job, created = scheduler.submit(spec)
    except SubmissionError as e:
        raise HTTPException(
            status_code=_REJECT_HTTP[e.reason],
            detail={"reason": e.reason.value, "detail": e.detail},
        )
    view = _job_view(job)
    view["created"] = created
    return view


@app.get("/jobs")
def list_jobs():
    return [_job_view(j) for j in scheduler.list_jobs()]


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    job = scheduler.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    return _job_view(job)


@app.post("/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    try:
        return _job_view(scheduler.cancel(job_id))
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")


@app.post("/jobs/{job_id}/tasks/{index}/succeed")
def succeed_task(job_id: str, index: int):
    try:
        return _job_view(scheduler.task_succeed(job_id, index))
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.post("/jobs/{job_id}/tasks/{index}/fail")
def fail_task(job_id: str, index: int, req: FailRequest | None = None):
    try:
        return _job_view(scheduler.task_fail(job_id, index, (req or FailRequest()).reason))
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))


@app.get("/cluster/usage")
def cluster_usage():
    u = scheduler.usage()
    return {
        "used_cpu": u.used_cpu,
        "used_mem_mb": u.used_mem_mb,
        "total_cpu": u.total_cpu,
        "total_mem_mb": u.total_mem_mb,
        "free_cpu": u.free_cpu,
        "free_mem_mb": u.free_mem_mb,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
