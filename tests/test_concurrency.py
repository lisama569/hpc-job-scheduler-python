"""并发提交与资源边界：超卖、整组原子性、并发幂等。

不变量（与线程交错顺序无关，结论稳定可复现）：
- 任何时刻 used_cpu <= total_cpu 且 used_mem_mb <= total_mem_mb；
- 每个 request_id 恰好对应一个作业；
- 多任务作业要么所有任务 RUNNING，要么所有任务 PENDING，不存在部分占用。
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from scheduler import JobSpec, JobState, ResourceRequest, TaskState
from scheduler.errors import RejectReason, SubmissionError

log = logging.getLogger("test.concurrency")

CLUSTER_CPU = 8
CLUSTER_MEM = 1024


def _spec(request_id: str, cpu: int = 1, mem_mb: int = 128, num_tasks: int = 1,
          user: str = "alice") -> JobSpec:
    return JobSpec(
        name=f"j-{request_id}", user=user, request_id=request_id,
        resources=ResourceRequest(cpu=cpu, mem_mb=mem_mb), num_tasks=num_tasks,
    )


def test_concurrent_submission_never_oversells(make_scheduler):
    s = make_scheduler(cpu=CLUSTER_CPU, mem_mb=CLUSTER_MEM)
    n = 32  # 32 个并发提交，每个 1cpu，集群只有 8cpu
    log.info("输入: %d 个线程并发提交 1cpu 作业, 集群容量 cpu=%d mem=%dMB",
             n, CLUSTER_CPU, CLUSTER_MEM)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda i: s.submit(_spec(f"c-{i}")), range(n)))

    jobs = s.list_jobs()
    running = [j for j in jobs if j.state is JobState.RUNNING]
    pending = [j for j in jobs if j.state is JobState.PENDING]
    usage = s.usage()
    log.info("判定依据: 新建=%d 运行=%d 等待=%d used_cpu=%d/%d used_mem=%d/%d",
             sum(1 for _, c in results if c), len(running), len(pending),
             usage.used_cpu, usage.total_cpu, usage.used_mem_mb, usage.total_mem_mb)

    assert len(jobs) == n                          # 每个 request_id 一个作业
    assert len(running) == CLUSTER_CPU             # 恰好占满，不多不少
    assert len(pending) == n - CLUSTER_CPU
    assert usage.used_cpu == CLUSTER_CPU           # 不超卖
    assert usage.used_mem_mb <= CLUSTER_MEM


def test_concurrent_gang_submission_is_atomic(make_scheduler):
    s = make_scheduler(cpu=CLUSTER_CPU, mem_mb=CLUSTER_MEM)
    n = 8  # 8 个并发整组作业，每个 4 任务 x 1cpu = 4cpu，集群 8cpu -> 恰好容纳 2 个
    log.info("输入: %d 个线程并发提交 4x1cpu 整组作业, 集群容量 %dcpu", n, CLUSTER_CPU)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: s.submit(_spec(f"g-{i}", num_tasks=4)), range(n)))

    jobs = s.list_jobs()
    running = [j for j in jobs if j.state is JobState.RUNNING]
    log.info("判定依据: 运行中整组=%d, 每个作业任务状态=%s",
             len(running),
             {j.job_id: [t.state.value for t in j.tasks] for j in jobs})

    assert len(running) == 2                       # 8cpu / 4cpu = 2 个整组
    for j in jobs:
        states = {t.state for t in j.tasks}
        # 整组原子性：不存在部分任务占用资源的情况
        assert len(states) == 1, f"{j.job_id} 出现部分调度: {states}"
        assert states in ({TaskState.RUNNING}, {TaskState.PENDING})
    assert s.usage().used_cpu == CLUSTER_CPU       # 不超卖


def test_concurrent_duplicate_request_yields_single_job(make_scheduler):
    s = make_scheduler()
    n = 16
    log.info("输入: %d 个线程用同一 request_id=dup-1 并发提交相同参数", n)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(lambda _: s.submit(_spec("dup-1")), range(n)))

    job_ids = {j.job_id for j, _ in results}
    created = sum(1 for _, c in results if c)
    log.info("判定依据: 去重后 job_id 数=%d, created=True 次数=%d, 作业总数=%d",
             len(job_ids), created, len(s.list_jobs()))

    assert len(job_ids) == 1
    assert created == 1
    assert len(s.list_jobs()) == 1


def test_concurrent_mixed_users_quota_isolated(make_scheduler):
    # 每用户配额 4cpu：alice 与 bob 各并发提交 8 个 1cpu 作业
    # 预期：每用户恰好 4 个被接纳，其余 4 个在提交时以 QUOTA_EXCEEDED 拒绝
    s = make_scheduler(cpu=64, mem_mb=65536, quota_cpu=4, quota_mem_mb=4096)
    log.info("输入: 两用户各 8 线程并发提交 1cpu 作业, 每用户配额 4cpu, 集群 64cpu")

    def submit_one(args):
        user, i = args
        try:
            job, _ = s.submit(_spec(f"m-{user}-{i}", user=user))
            return (user, "admitted", job.job_id)
        except SubmissionError as e:
            return (user, e.reason, None)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(submit_one, [(u, i) for u in ("alice", "bob") for i in range(8)]))

    for user in ("alice", "bob"):
        mine = [r for r in results if r[0] == user]
        admitted = [r for r in mine if r[1] == "admitted"]
        rejected = [r for r in mine if r[1] is RejectReason.QUOTA_EXCEEDED]
        log.info("判定依据: 用户 %s 接纳=%d 配额拒绝=%d (配额 4cpu, 提交 8 个 1cpu)",
                 user, len(admitted), len(rejected))
        assert len(admitted) == 4
        assert len(rejected) == 4
        running = [j for j in s.list_jobs()
                   if j.spec.user == user and j.state is JobState.RUNNING]
        assert len(running) == 4                   # 配额约束生效且不超过
