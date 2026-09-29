"""调度引擎：提交校验、幂等、配额、整组调度、依赖与数组推进。

一致性约定：
- 所有改变状态的公共方法都在 ``self._lock``（可重入锁）内执行，
  因此"校验配额 / 检查依赖 / 占用资源"之间不会被并发请求插入。
- 每次状态变化后调用 ``_promote()`` 做一次确定性的推进：
  按提交顺序扫描 PENDING 作业，依赖未就绪或资源不足的跳过（继续等待），
  其余整组占用资源进入 RUNNING。同一事件序列必然得到同一结果。
"""

from __future__ import annotations

import threading

from scheduler.cluster import Cluster, ClusterUsage
from scheduler.errors import RejectReason, SubmissionError
from scheduler.models import (
    FailureKind,
    Job,
    JobSpec,
    JobState,
    Task,
    TaskState,
)

_TERMINAL = (JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED)


class Scheduler:
    """本地作业调度器。"""

    def __init__(self, cluster: Cluster, quota_cpu: int, quota_mem_mb: int) -> None:
        self._cluster = cluster
        self._quota_cpu = quota_cpu
        self._quota_mem_mb = quota_mem_mb
        self._jobs: dict[str, Job] = {}
        self._order: list[str] = []            # 提交顺序，用于确定性推进
        self._by_request_id: dict[str, str] = {}
        self._lock = threading.RLock()
        self._seq = 0

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def submit(self, spec: JobSpec) -> tuple[Job, bool]:
        """提交作业，返回 (作业, 是否新建)。

        同一 request_id 重复提交且参数一致时返回已有作业（幂等）；
        参数不一致则拒绝。各类拒绝原因通过 SubmissionError.reason 区分。
        """
        with self._lock:
            existing_id = self._by_request_id.get(spec.request_id)
            if existing_id is not None:
                existing = self._jobs[existing_id]
                if self._same_spec(existing.spec, spec):
                    return existing, False
                raise SubmissionError(
                    RejectReason.DUPLICATE_REQUEST,
                    f"request_id={spec.request_id!r} 已存在但参数不一致",
                )

            self._validate(spec)

            self._seq += 1
            job = Job(
                job_id=f"job-{self._seq}",
                spec=spec,
                tasks=[Task(index=i) for i in range(spec.num_tasks)],
            )
            self._jobs[job.job_id] = job
            self._order.append(job.job_id)
            self._by_request_id[spec.request_id] = job.job_id
            self._promote()
            return job, True

    @staticmethod
    def _same_spec(a: JobSpec, b: JobSpec) -> bool:
        return (
            a.name == b.name
            and a.user == b.user
            and a.resources == b.resources
            and a.num_tasks == b.num_tasks
            and a.depends_on == b.depends_on
            and a.max_retries == b.max_retries
        )

    def _validate(self, spec: JobSpec) -> None:
        # 1) 资源参数非法
        if spec.num_tasks < 1:
            raise SubmissionError(RejectReason.INVALID_RESOURCES, "num_tasks 必须 >= 1")
        if spec.max_retries < 0:
            raise SubmissionError(RejectReason.INVALID_RESOURCES, "max_retries 必须 >= 0")
        if not spec.request_id:
            raise SubmissionError(RejectReason.INVALID_RESOURCES, "request_id 不能为空")
        if not self._cluster.fits(spec.resources.cpu, spec.resources.mem_mb):
            raise SubmissionError(
                RejectReason.INVALID_RESOURCES,
                f"单任务资源 cpu={spec.resources.cpu} mem={spec.resources.mem_mb}MB "
                f"非法或超出集群容量",
            )
        # 整组总量必须可能容纳，否则永远不会被调度，直接判非法
        total_cpu = spec.resources.cpu * spec.num_tasks
        total_mem = spec.resources.mem_mb * spec.num_tasks
        if not self._cluster.fits(total_cpu, total_mem):
            raise SubmissionError(
                RejectReason.INVALID_RESOURCES,
                f"整组资源 cpu={total_cpu} mem={total_mem}MB 超出集群容量，永远无法调度",
            )

        # 2) 依赖不存在
        for dep in spec.depends_on:
            if dep not in self._jobs:
                raise SubmissionError(
                    RejectReason.UNKNOWN_DEPENDENCY, f"依赖的作业 {dep!r} 不存在"
                )

        # 3) 用户配额：已占用（PENDING/RUNNING）+ 本次申请 <= 配额
        used_cpu = used_mem = 0
        for job in self._jobs.values():
            if job.spec.user == spec.user and job.state not in _TERMINAL:
                used_cpu += job.total_cpu
                used_mem += job.total_mem_mb
        if used_cpu + total_cpu > self._quota_cpu or used_mem + total_mem > self._quota_mem_mb:
            raise SubmissionError(
                RejectReason.QUOTA_EXCEEDED,
                f"用户 {spec.user!r} 配额不足: 已占 cpu={used_cpu} mem={used_mem}MB, "
                f"申请 cpu={total_cpu} mem={total_mem}MB, "
                f"配额 cpu={self._quota_cpu} mem={self._quota_mem_mb}MB",
            )

    # ------------------------------------------------------------------
    # 生命周期推进（模拟执行）
    # ------------------------------------------------------------------
    def task_succeed(self, job_id: str, index: int) -> Job:
        """标记数组元素成功；全部成功则作业成功并释放资源。"""
        with self._lock:
            job = self._require_running(job_id)
            task = job.tasks[index]
            if task.state is not TaskState.RUNNING:
                raise ValueError(f"任务 {index} 当前状态 {task.state.value}，不能标记成功")
            task.state = TaskState.SUCCEEDED
            if all(t.state is TaskState.SUCCEEDED for t in job.tasks):
                self._finish(job, JobState.SUCCEEDED)
            return job

    def task_fail(self, job_id: str, index: int, reason: str = "task error") -> Job:
        """标记数组元素失败；重试耗尽则整个作业失败并释放资源。"""
        with self._lock:
            job = self._require_running(job_id)
            task = job.tasks[index]
            if task.state is not TaskState.RUNNING:
                raise ValueError(f"任务 {index} 当前状态 {task.state.value}，不能标记失败")
            if task.attempt <= job.spec.max_retries:
                # 还有重试额度：资源仍由整组持有，任务原地重跑
                task.attempt += 1
                task.failure_reason = reason
                return job
            task.state = TaskState.FAILED
            task.failure_reason = reason
            for t in job.tasks:
                if t.state is TaskState.RUNNING:
                    t.state = TaskState.CANCELLED  # 同组其余任务随作业终止
            job.failure_kind = FailureKind.TASK_FAILED
            job.failure_reason = f"任务 {index} 失败且重试耗尽: {reason}"
            self._finish(job, JobState.FAILED)
            return job

    def cancel(self, job_id: str) -> Job:
        """取消作业；终态作业取消为幂等空操作。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"作业 {job_id!r} 不存在")
            if job.state in _TERMINAL:
                return job
            for t in job.tasks:
                if t.state in (TaskState.PENDING, TaskState.RUNNING):
                    t.state = TaskState.CANCELLED
            self._finish(job, JobState.CANCELLED)
            return job

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(self) -> list[Job]:
        with self._lock:
            return [self._jobs[jid] for jid in self._order]

    def usage(self) -> ClusterUsage:
        return self._cluster.usage()

    # ------------------------------------------------------------------
    # 内部：推进与收尾
    # ------------------------------------------------------------------
    def _promote(self) -> None:
        """按提交顺序确定性推进 PENDING 作业（调用方须持锁）。"""
        for jid in self._order:
            job = self._jobs[jid]
            if job.state is not JobState.PENDING:
                continue
            # 依赖判定：上游失败/取消不得静默执行下游
            blocked = False
            for dep_id in job.spec.depends_on:
                dep = self._jobs[dep_id]
                if dep.state is JobState.FAILED:
                    job.failure_kind = FailureKind.DEPENDENCY_FAILED
                    job.failure_reason = f"依赖的作业 {dep_id} 失败: {dep.failure_reason}"
                    self._finish(job, JobState.FAILED)
                    blocked = True
                    break
                if dep.state is JobState.CANCELLED:
                    job.failure_kind = FailureKind.DEPENDENCY_CANCELLED
                    job.failure_reason = f"依赖的作业 {dep_id} 被取消"
                    self._finish(job, JobState.CANCELLED)
                    blocked = True
                    break
                if dep.state is not JobState.SUCCEEDED:
                    blocked = True  # 依赖未就绪，继续等待
                    break
            if blocked or job.state is not JobState.PENDING:
                continue
            # 整组原子调度：一次申请全部任务资源，失败则整体继续等待
            if self._cluster.try_allocate(job.total_cpu, job.total_mem_mb):
                job.state = JobState.RUNNING
                for t in job.tasks:
                    t.state = TaskState.RUNNING
                    t.attempt = 1

    def _finish(self, job: Job, state: JobState) -> None:
        """进入终态：释放资源（若在运行）并级联推进（调用方须持锁）。"""
        if job.state is JobState.RUNNING:
            self._cluster.release(job.total_cpu, job.total_mem_mb)
        job.state = state
        self._promote()

    def _require_running(self, job_id: str) -> Job:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"作业 {job_id!r} 不存在")
        if job.state is not JobState.RUNNING:
            raise ValueError(f"作业 {job_id} 当前状态 {job.state.value}，不在运行中")
        return job
