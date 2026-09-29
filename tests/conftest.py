"""pytest 公共配置：日志格式与调度器工厂。"""

from __future__ import annotations

import logging

import pytest

from scheduler import Scheduler
from scheduler.cluster import Cluster, ClusterCapacity

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)

log = logging.getLogger("test.scheduler")

# 默认测试集群：8 CPU / 1024MB，配额与集群容量一致
CLUSTER_CPU = 8
CLUSTER_MEM_MB = 1024


@pytest.fixture()
def make_scheduler():
    """返回一个工厂，便于按用例定制容量与配额。"""

    def _make(
        cpu: int = CLUSTER_CPU,
        mem_mb: int = CLUSTER_MEM_MB,
        quota_cpu: int | None = None,
        quota_mem_mb: int | None = None,
    ) -> Scheduler:
        return Scheduler(
            Cluster(ClusterCapacity(total_cpu=cpu, total_mem_mb=mem_mb)),
            quota_cpu=quota_cpu if quota_cpu is not None else cpu * 4,
            quota_mem_mb=quota_mem_mb if quota_mem_mb is not None else mem_mb * 4,
        )

    return _make
