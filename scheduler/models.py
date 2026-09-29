"""数据模型：资源、作业、任务与状态枚举（骨架，后续填充）。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class JobState(enum.Enum):
    """作业生命周期状态。"""

    PENDING = "PENDING"            # 已接纳，等待依赖或资源
    RUNNING = "RUNNING"            # 已获得资源，正在运行
    SUCCEEDED = "SUCCEEDED"        # 全部任务成功
    FAILED = "FAILED"              # 至少一个任务失败（含依赖失败）
    CANCELLED = "CANCELLED"        # 被取消（含依赖取消导致的取消）


class TaskState(enum.Enum):
    """数组任务（作业元素）状态。"""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FailureKind(enum.Enum):
    """失败原因归类，避免笼统报错。"""

    TASK_FAILED = "TASK_FAILED"                    # 任务自身失败且重试耗尽
    DEPENDENCY_FAILED = "DEPENDENCY_FAILED"        # 上游作业失败
    DEPENDENCY_CANCELLED = "DEPENDENCY_CANCELLED"  # 上游作业被取消


@dataclass(frozen=True)
class ResourceRequest:
    """单个任务所需的资源。"""

    cpu: int = 1
    mem_mb: int = 128


@dataclass
class Task:
    """作业内的一个任务（数组元素）。"""

    index: int
    state: TaskState = TaskState.PENDING
    attempt: int = 0
    failure_reason: str | None = None


@dataclass
class JobSpec:
    """提交作业的描述。"""

    name: str
    user: str
    request_id: str                      # 幂等键：同一 request_id 只产生一个作业
    resources: ResourceRequest = field(default_factory=ResourceRequest)
    num_tasks: int = 1                   # >1 时为作业数组，整组原子调度
    depends_on: tuple[str, ...] = ()     # 依赖的作业 ID
    max_retries: int = 0                 # 每个任务允许的重试次数


@dataclass
class Job:
    """调度器内部维护的作业实体。"""

    job_id: str
    spec: JobSpec
    state: JobState = JobState.PENDING
    tasks: list[Task] = field(default_factory=list)
    failure_kind: FailureKind | None = None
    failure_reason: str | None = None

    @property
    def total_cpu(self) -> int:
        return self.spec.resources.cpu * self.spec.num_tasks

    @property
    def total_mem_mb(self) -> int:
        return self.spec.resources.mem_mb * self.spec.num_tasks
