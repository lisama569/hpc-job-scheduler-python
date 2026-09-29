"""并发提交与资源边界测试：结论必须稳定可复现，日志打印输入与判定依据。"""
import logging
from concurrent.futures import ThreadPoolExecutor

from scheduler.cluster import ClusterState
from scheduler.models import JobState, Resources, SubmitStatus, TaskSpec
from scheduler.scheduler import Scheduler

log = logging.getLogger("test.concurrency")


def spec(name, cpu=1, mem_mb=1024):
    return TaskSpec(name=name, resources=Resources(cpu, mem_mb))


def test_concurrent_submit_never_oversells():
    """20 个线程各提交 1 CPU 作业，容量 8：运行数恰好 8，绝不超卖。"""
    cluster = ClusterState(Resources(cpu=8, mem_mb=65536))
    sched = Scheduler(cluster)
    n = 20
    results = []
    with ThreadPoolExecutor(max_workers=n) as pool:
        futs = [pool.submit(sched.submit, "alice", f"req-{i}", [spec("t")])
                for i in range(n)]
        results = [f.result() for f in futs]

    accepted = [r for r in results if r.status is SubmitStatus.ACCEPTED]
    running = [j for j in sched.list_jobs() if j.state is JobState.RUNNING]
    pending = [j for j in sched.list_jobs() if j.state is JobState.PENDING]
    used = cluster.snapshot()["used"]["cpu"]
    log.info("输入: %d 线程并发提交 1CPU 作业, 容量=8 | 判定: accepted=%d, "
             "running=%d, pending=%d, used=%d <= 8",
             n, len(accepted), len(running), len(pending), used)
    assert len(accepted) == n
    assert len(running) == 8
    assert len(pending) == n - 8
    assert used == 8


def test_concurrent_duplicate_submit_single_job():
    """多线程用同一 request_id 提交：只产生一个作业，其余全部幂等命中。"""
    cluster = ClusterState(Resources(cpu=8, mem_mb=65536))
    sched = Scheduler(cluster)
    n = 16
    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(
            lambda _: sched.submit("alice", "same-req", [spec("t")]), range(n)))

    accepted = [r for r in results if r.status is SubmitStatus.ACCEPTED]
    duplicates = [r for r in results if r.status is SubmitStatus.DUPLICATE]
    job_ids = {r.job.job_id for r in results}
    log.info("输入: %d 线程同 request_id 提交 | 判定: accepted=%d, duplicate=%d, "
             "不同 job_id 数=%d, 总作业数=%d",
             n, len(accepted), len(duplicates), len(job_ids), len(sched.list_jobs()))
    assert len(accepted) == 1
    assert len(duplicates) == n - 1
    assert len(job_ids) == 1
    assert len(sched.list_jobs()) == 1


def test_concurrent_mixed_submit_and_cancel_consistent():
    """并发提交与取消交错：最终资源账必须归零或守恒，状态必须终态一致。"""
    cluster = ClusterState(Resources(cpu=8, mem_mb=65536))
    sched = Scheduler(cluster)
    n = 12
    with ThreadPoolExecutor(max_workers=n) as pool:
        submit_futs = [pool.submit(sched.submit, "alice", f"req-{i}", [spec("t")])
                       for i in range(n)]
        jobs = [f.result().job for f in submit_futs]
        cancel_futs = [pool.submit(sched.cancel, j.job_id) for j in jobs]
        for f in cancel_futs:
            f.result()

    states = [j.state for j in sched.list_jobs()]
    used = cluster.snapshot()["used"]["cpu"]
    log.info("输入: %d 作业提交后全部并发取消 | 判定: 全部 CANCELLED=%s, used=%d == 0",
             n, all(s is JobState.CANCELLED for s in states), used)
    assert all(s is JobState.CANCELLED for s in states)
    assert used == 0


def test_quota_not_oversold_under_concurrency():
    """配额边界：用户配额 4 CPU，10 个并发 1 CPU 作业，运行数不超过 4。"""
    cluster = ClusterState(Resources(cpu=64, mem_mb=65536),
                           quotas={"bob": Resources(cpu=4, mem_mb=65536)})
    sched = Scheduler(cluster)
    n = 10
    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda i: sched.submit("bob", f"req-{i}", [spec("t")]),
                      range(n)))
    running = [j for j in sched.list_jobs() if j.state is JobState.RUNNING]
    used_bob = cluster.snapshot()["used_by_user"].get("bob", {}).get("cpu", 0)
    log.info("输入: bob 并发提交 %d 个 1CPU 作业, 配额=4 | 判定: running=%d <= 4, "
             "used_by_bob=%d", n, len(running), used_bob)
    assert len(running) == 4
    assert used_bob == 4


def test_dependency_failure_deterministic_under_concurrency():
    """并发完成/失败上游：下游要么运行要么被阻断，结果由上游终态唯一决定。"""
    for trial in range(10):
        cluster = ClusterState(Resources(cpu=8, mem_mb=65536))
        sched = Scheduler(cluster)
        up = sched.submit("alice", f"up-{trial}", [spec("t")]).job
        down = sched.submit("alice", f"down-{trial}", [spec("t")],
                            depends_on=[up.job_id]).job
        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(sched.complete_task, up.job_id, 0, False, "boom")
            f2 = pool.submit(sched.cancel, down.job_id)
            f1.result()
            f2.result()
        log.info("trial=%d 输入: 上游失败与下游取消并发 | 判定: up=%s down=%s "
                 "(终态, 非 RUNNING)", trial, up.state.value, down.state.value)
        assert up.state is JobState.FAILED
        # 下游无论取消与阻断谁先发生，都不得进入 RUNNING/SUCCEEDED
        assert down.state in (JobState.BLOCKED, JobState.CANCELLED)
        assert cluster.snapshot()["used"]["cpu"] == 0
