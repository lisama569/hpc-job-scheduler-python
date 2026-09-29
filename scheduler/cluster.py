"""本地集群状态：节点容量、已用资源、用户配额。"""
from __future__ import annotations

import threading

from .models import Resources


class ClusterState:
    """用本地状态模拟集群容量与配额，所有变更在锁内完成。"""

    def __init__(self, capacity: Resources, quotas: dict[str, Resources] | None = None,
                 default_quota: Resources | None = None) -> None:
        self._lock = threading.RLock()
        self._capacity = capacity
        self._used = Resources.zero()
        self._default_quota = default_quota if default_quota is not None else capacity
        self._quotas: dict[str, Resources] = dict(quotas or {})
        self._used_by_user: dict[str, Resources] = {}

    @property
    def capacity(self) -> Resources:
        return self._capacity

    def quota_of(self, user: str) -> Resources:
        return self._quotas.get(user, self._default_quota)

    def used_of(self, user: str) -> Resources:
        return self._used_by_user.get(user, Resources.zero())

    def can_allocate(self, user: str, req: Resources) -> bool:
        """并发安全地检查：集群容量与用户配额是否同时满足（不超卖）。"""
        with self._lock:
            return (self._used + req).fits_in(self._capacity) and \
                   (self.used_of(user) + req).fits_in(self.quota_of(user))

    def try_allocate(self, user: str, req: Resources) -> bool:
        """原子地检查并占用资源；成功返回 True，否则不产生任何占用。"""
        with self._lock:
            if not self.can_allocate(user, req):
                return False
            self._used = self._used + req
            self._used_by_user[user] = self.used_of(user) + req
            return True

    def release(self, user: str, req: Resources) -> None:
        with self._lock:
            self._used = self._used - req
            self._used_by_user[user] = self.used_of(user) - req

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "capacity": {"cpu": self._capacity.cpu, "mem_mb": self._capacity.mem_mb},
                "used": {"cpu": self._used.cpu, "mem_mb": self._used.mem_mb},
                "used_by_user": {
                    u: {"cpu": r.cpu, "mem_mb": r.mem_mb}
                    for u, r in self._used_by_user.items()
                },
            }
