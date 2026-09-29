"""数据模型与错误分类：作业、任务、资源、拒绝原因。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field


class RejectReason(enum.Enum):
    """提交被拒绝的分类原因，彼此可区分。"""

    INVALID_RESOURCES = "invalid_resources"        # 资源参数非法（非正数等）
    EXCEEDS_CAPACITY = "exceeds_capacity"          # 单任务超过集群物理容量
    EXCEEDS_QUOTA = "exceeds_quota"                # 超出用户配额
    DEPENDENCY_NOT_FOUND = "dependency_not_found"  # 依赖的作业不存在


class DependencyFailure(enum.Enum):
    """依赖未满足时的失败归类。"""

    UPSTREAM_FAILED = "upstream_failed"
    UPSTREAM_CANCELLED = "upstream_cancelled"


class SubmitStatus(enum.Enum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"      # 幂等命中：返回已有作业
    REJECTED = "rejected"


class JobState(enum.Enum):
    PENDING = "pending"      # 已接纳，等待整组资源
    RUNNING = "running"      # 整组已占用资源
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"      # 因上游失败/取消而不得执行


class TaskState(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BLOCKED = "blocked"


TERMINAL_JOB_STATES = frozenset(
    {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED, JobState.BLOCKED}
)
TERMINAL_TASK_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED, TaskState.BLOCKED}
)


@dataclass(frozen=True)
class Resources:
    """一份资源需求/容量。"""

    cpu: int
    mem_mb: int

    def fits_in(self, capacity: "Resources") -> bool:
        return self.cpu <= capacity.cpu and self.mem_mb <= capacity.mem_mb

    def __add__(self, other: "Resources") -> "Resources":
        return Resources(cpu=self.cpu + other.cpu, mem_mb=self.mem_mb + other.mem_mb)

    def __sub__(self, other: "Resources") -> "Resources":
        return Resources(cpu=self.cpu - other.cpu, mem_mb=self.mem_mb - other.mem_mb)

    def __le__(self, other: "Resources") -> bool:
        return self.cpu <= other.cpu and self.mem_mb <= other.mem_mb

    @staticmethod
    def zero() -> "Resources":
        return Resources(cpu=0, mem_mb=0)


@dataclass
class TaskSpec:
    """单个任务的规格（数组元素）。"""

    name: str
    resources: Resources
    max_retries: int = 0


@dataclass
class Task:
    """任务运行时状态。"""

    spec: TaskSpec
    index: int
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    last_error: str | None = None


@dataclass
class Job:
    """作业：一组需要原子调度的任务。"""

    job_id: str
    owner: str
    request_id: str
    tasks: list[Task]
    depends_on: list[str] = field(default_factory=list)
    state: JobState = JobState.PENDING
    failure_reason: DependencyFailure | None = None

    @property
    def group_resources(self) -> Resources:
        """整组资源需求 = 各任务需求之和（gang 语义）。"""
        total = Resources.zero()
        for t in self.tasks:
            total = total + t.spec.resources
        return total
