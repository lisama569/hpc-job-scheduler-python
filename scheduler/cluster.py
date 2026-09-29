"""本地集群资源视图：用内存状态模拟节点容量。

所有资源核算都在同一把锁内完成，保证并发提交时不会超卖：
``try_allocate`` 的"检查 + 扣减"是一个原子操作。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass


@dataclass(frozen=True)
class ClusterCapacity:
    """集群总容量。"""

    total_cpu: int
    total_mem_mb: int


@dataclass(frozen=True)
class ClusterUsage:
    """当前资源使用快照。"""

    used_cpu: int
    used_mem_mb: int
    total_cpu: int
    total_mem_mb: int

    @property
    def free_cpu(self) -> int:
        return self.total_cpu - self.used_cpu

    @property
    def free_mem_mb(self) -> int:
        return self.total_mem_mb - self.used_mem_mb


class Cluster:
    """本地集群：集中式资源账本。

    整组（gang）调度通过一次 ``try_allocate`` 申请全部任务的资源总量，
    要么全部成功、要么什么都不扣，天然满足"整体接纳或整体等待"。
    """

    def __init__(self, capacity: ClusterCapacity) -> None:
        if capacity.total_cpu <= 0 or capacity.total_mem_mb <= 0:
            raise ValueError("集群容量必须为正数")
        self._capacity = capacity
        self._used_cpu = 0
        self._used_mem_mb = 0
        self._lock = threading.Lock()

    @property
    def capacity(self) -> ClusterCapacity:
        return self._capacity

    def fits(self, cpu: int, mem_mb: int) -> bool:
        """检查给定资源量是否不超过单机/集群可分配上限（用于参数校验）。"""
        return 0 < cpu <= self._capacity.total_cpu and 0 < mem_mb <= self._capacity.total_mem_mb

    def try_allocate(self, cpu: int, mem_mb: int) -> bool:
        """原子地尝试占用资源：成功返回 True，失败则不改变任何状态。"""
        with self._lock:
            if (
                self._used_cpu + cpu > self._capacity.total_cpu
                or self._used_mem_mb + mem_mb > self._capacity.total_mem_mb
            ):
                return False
            self._used_cpu += cpu
            self._used_mem_mb += mem_mb
            return True

    def release(self, cpu: int, mem_mb: int) -> None:
        """释放资源；释放量超过占用量属于调用方 bug，直接报错。"""
        with self._lock:
            if cpu > self._used_cpu or mem_mb > self._used_mem_mb:
                raise RuntimeError(
                    f"释放量超过占用量: cpu={cpu}/{self._used_cpu}, "
                    f"mem={mem_mb}/{self._used_mem_mb}"
                )
            self._used_cpu -= cpu
            self._used_mem_mb -= mem_mb

    def usage(self) -> ClusterUsage:
        with self._lock:
            return ClusterUsage(
                used_cpu=self._used_cpu,
                used_mem_mb=self._used_mem_mb,
                total_cpu=self._capacity.total_cpu,
                total_mem_mb=self._capacity.total_mem_mb,
            )
