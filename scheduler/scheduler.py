"""调度器核心：提交去重、校验、整组（gang）调度、依赖推进、取消。

一致性约定：
- 所有状态变更都在同一把锁内完成，调度循环在每次变更后重跑，
  因此并发提交、完成、取消交错时结果稳定可复现。
- 整组调度：一个作业的全部任务同时占用资源，否则整体等待，绝不部分占用。
"""
from __future__ import annotations

import itertools
import threading
from dataclasses import dataclass

from .cluster import ClusterState
from .errors import SchedulerError
from .models import (
    DependencyFailure,
    Job,
    JobState,
    RejectReason,
    Resources,
    SubmitStatus,
    Task,
    TaskSpec,
    TaskState,
    TERMINAL_JOB_STATES,
    TERMINAL_TASK_STATES,
)


@dataclass(frozen=True)
class SubmitResult:
    status: SubmitStatus
    job: Job | None = None
    reason: RejectReason | None = None


class Scheduler:
    def __init__(self, cluster: ClusterState) -> None:
        self._cluster = cluster
        self._lock = threading.RLock()
        self._jobs: dict[str, Job] = {}
        self._by_request: dict[str, str] = {}  # request_id -> job_id（幂等去重）
        self._seq = itertools.count(1)

    # ------------------------------------------------------------------ submit
    def submit(self, owner: str, request_id: str, specs: list[TaskSpec],
               depends_on: list[str] | None = None) -> SubmitResult:
        """提交作业。同一 request_id 重复提交返回已有作业（DUPLICATE）。"""
        depends_on = list(depends_on or [])
        with self._lock:
            existing = self._by_request.get(request_id)
            if existing is not None:
                return SubmitResult(SubmitStatus.DUPLICATE, job=self._jobs[existing])

            reason = self._validate(owner, specs, depends_on)
            if reason is not None:
                return SubmitResult(SubmitStatus.REJECTED, reason=reason)

            job = Job(
                job_id=f"job-{next(self._seq)}",
                owner=owner,
                request_id=request_id,
                tasks=[Task(spec=s, index=i) for i, s in enumerate(specs)],
                depends_on=depends_on,
            )
            self._jobs[job.job_id] = job
            self._by_request[request_id] = job.job_id
            self._schedule_locked()
            return SubmitResult(SubmitStatus.ACCEPTED, job=job)

    def _validate(self, owner: str, specs: list[TaskSpec],
                  depends_on: list[str]) -> RejectReason | None:
        if not specs:
            return RejectReason.INVALID_RESOURCES
        for s in specs:
            if s.resources.cpu <= 0 or s.resources.mem_mb <= 0:
                return RejectReason.INVALID_RESOURCES
            if not s.resources.fits_in(self._cluster.capacity):
                return RejectReason.EXCEEDS_CAPACITY
            if not s.resources.fits_in(self._cluster.quota_of(owner)):
                return RejectReason.EXCEEDS_QUOTA
        for dep in depends_on:
            if dep not in self._jobs:
                return RejectReason.DEPENDENCY_NOT_FOUND
        return None

    # ------------------------------------------------------------- scheduling
    def _schedule_locked(self) -> None:
        """推进依赖并尝试整组调度；在锁内循环直到没有变化。"""
        changed = True
        while changed:
            changed = self._propagate_dependencies_locked()
            changed = self._try_start_pending_locked() or changed

    def _propagate_dependencies_locked(self) -> bool:
        """上游失败/取消的作业，下游标记为 BLOCKED，绝不静默执行。"""
        changed = False
        for job in self._jobs.values():
            if job.state is not JobState.PENDING:
                continue
            for dep_id in job.depends_on:
                dep = self._jobs[dep_id]
                if dep.state is JobState.FAILED:
                    self._block_locked(job, DependencyFailure.UPSTREAM_FAILED)
                    changed = True
                    break
                if dep.state is JobState.CANCELLED:
                    self._block_locked(job, DependencyFailure.UPSTREAM_CANCELLED)
                    changed = True
                    break
        return changed

    def _block_locked(self, job: Job, reason: DependencyFailure) -> None:
        job.state = JobState.BLOCKED
        job.failure_reason = reason
        for t in job.tasks:
            if t.state not in TERMINAL_TASK_STATES:
                t.state = TaskState.BLOCKED

    def _deps_satisfied(self, job: Job) -> bool:
        return all(self._jobs[d].state is JobState.SUCCEEDED for d in job.depends_on)

    def _try_start_pending_locked(self) -> bool:
        """按提交顺序尝试整组启动；资源不足则整体继续等待（不超卖）。"""
        started_any = False
        for job in self._jobs.values():
            if job.state is not JobState.PENDING or not self._deps_satisfied(job):
                continue
            if self._cluster.try_allocate(job.owner, job.group_resources):
                job.state = JobState.RUNNING
                for t in job.tasks:
                    t.state = TaskState.RUNNING
                    t.attempt = 1
                started_any = True
        return started_any

    # ----------------------------------------------------------------- events
    def complete_task(self, job_id: str, task_index: int, ok: bool,
                      error: str | None = None) -> None:
        """汇报任务结束。失败时在重试额度内重新排队，否则作业失败。"""
        with self._lock:
            job, task = self._get_task(job_id, task_index)
            if task.state is not TaskState.RUNNING:
                raise SchedulerError(f"task {job_id}[{task_index}] not running")
            if ok:
                task.state = TaskState.SUCCEEDED
            else:
                task.last_error = error
                if task.attempt <= task.spec.max_retries:
                    task.attempt += 1
                    task.state = TaskState.RUNNING  # 资源仍被整组持有，原地重试
                    return
                task.state = TaskState.FAILED
            self._finalize_if_done_locked(job)
            self._schedule_locked()

    def _finalize_if_done_locked(self, job: Job) -> None:
        states = {t.state for t in job.tasks}
        if states <= {TaskState.SUCCEEDED}:
            job.state = JobState.SUCCEEDED
        elif TaskState.FAILED in states and not (states & {TaskState.RUNNING, TaskState.PENDING}):
            job.state = JobState.FAILED
        elif TaskState.CANCELLED in states and not (states & {TaskState.RUNNING, TaskState.PENDING}):
            job.state = JobState.CANCELLED
        else:
            return
        self._cluster.release(job.owner, job.group_resources)

    def cancel(self, job_id: str) -> Job:
        """取消作业：释放整组资源，级联取消/阻断下游。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise SchedulerError(f"unknown job {job_id}")
            if job.state in TERMINAL_JOB_STATES:
                return job
            if job.state is JobState.RUNNING:
                self._cluster.release(job.owner, job.group_resources)
            job.state = JobState.CANCELLED
            for t in job.tasks:
                if t.state not in TERMINAL_TASK_STATES:
                    t.state = TaskState.CANCELLED
            self._schedule_locked()
            return job

    # ----------------------------------------------------------------- query
    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def _get_task(self, job_id: str, task_index: int) -> tuple[Job, Task]:
        job = self._jobs.get(job_id)
        if job is None:
            raise SchedulerError(f"unknown job {job_id}")
        try:
            return job, job.tasks[task_index]
        except IndexError:
            raise SchedulerError(f"unknown task {job_id}[{task_index}]") from None
