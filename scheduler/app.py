"""FastAPI 接口层：把调度器暴露为 HTTP API，不改动既有根路由。"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .cluster import ClusterState
from .models import Job, Resources, SubmitStatus, TaskSpec
from .scheduler import Scheduler
from .errors import SchedulerError

# 本地模拟集群：8 CPU / 16GiB；默认配额即整集群容量。
default_cluster = ClusterState(capacity=Resources(cpu=8, mem_mb=16384))
default_scheduler = Scheduler(default_cluster)


class ResourcesIn(BaseModel):
    cpu: int
    mem_mb: int


class TaskSpecIn(BaseModel):
    name: str
    resources: ResourcesIn
    max_retries: int = Field(default=0, ge=0)


class SubmitRequest(BaseModel):
    owner: str
    request_id: str
    tasks: list[TaskSpecIn]
    depends_on: list[str] = []


class CompleteRequest(BaseModel):
    task_index: int
    ok: bool
    error: str | None = None


def _job_view(job: Job) -> dict:
    return {
        "job_id": job.job_id,
        "owner": job.owner,
        "request_id": job.request_id,
        "state": job.state.value,
        "depends_on": job.depends_on,
        "failure_reason": job.failure_reason.value if job.failure_reason else None,
        "tasks": [
            {
                "index": t.index,
                "name": t.spec.name,
                "state": t.state.value,
                "attempt": t.attempt,
                "last_error": t.last_error,
            }
            for t in job.tasks
        ],
    }


def create_app(scheduler: Scheduler | None = None,
               cluster: ClusterState | None = None) -> FastAPI:
    """构建子应用；默认使用模块级单例，测试可注入独立实例。"""
    scheduler = scheduler if scheduler is not None else default_scheduler
    cluster = cluster if cluster is not None else default_cluster
    app = FastAPI(title="local-hpc-scheduler")

    @app.post("/jobs", status_code=201)
    def submit_job(req: SubmitRequest) -> dict:
        specs = [
            TaskSpec(name=t.name,
                     resources=Resources(cpu=t.resources.cpu, mem_mb=t.resources.mem_mb),
                     max_retries=t.max_retries)
            for t in req.tasks
        ]
        result = scheduler.submit(req.owner, req.request_id, specs, req.depends_on)
        if result.status is SubmitStatus.REJECTED:
            # 拒绝原因分类体现在 HTTP 状态码与 detail 中，彼此可区分。
            assert result.reason is not None
            raise HTTPException(status_code=422,
                                detail={"reason": result.reason.value})
        assert result.job is not None
        return {"status": result.status.value, "job": _job_view(result.job)}

    @app.get("/jobs")
    def list_jobs() -> list[dict]:
        return [_job_view(j) for j in scheduler.list_jobs()]

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str) -> dict:
        job = scheduler.get_job(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return _job_view(job)

    @app.post("/jobs/{job_id}/complete")
    def complete_task(job_id: str, req: CompleteRequest) -> dict:
        try:
            scheduler.complete_task(job_id, req.task_index, req.ok, req.error)
        except SchedulerError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        job = scheduler.get_job(job_id)
        assert job is not None
        return _job_view(job)

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> dict:
        try:
            job = scheduler.cancel(job_id)
        except SchedulerError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return _job_view(job)

    @app.get("/cluster")
    def cluster_state() -> dict:
        return cluster.snapshot()

    return app
