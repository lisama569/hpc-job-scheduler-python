"""核心调度语义测试：每个用例打印输入与判定依据，结论可复现。"""
import logging

from scheduler.cluster import ClusterState
from scheduler.models import (
    DependencyFailure,
    JobState,
    RejectReason,
    Resources,
    SubmitStatus,
    TaskSpec,
    TaskState,
)
from scheduler.scheduler import Scheduler

log = logging.getLogger("test.scheduler")


def make_scheduler(cpu=4, mem_mb=8192, quotas=None, default_quota=None):
    cluster = ClusterState(Resources(cpu, mem_mb), quotas=quotas,
                           default_quota=default_quota)
    return Scheduler(cluster), cluster


def spec(name, cpu=1, mem_mb=1024, retries=0):
    return TaskSpec(name=name, resources=Resources(cpu, mem_mb), max_retries=retries)


def test_duplicate_request_id_returns_same_job():
    sched, _ = make_scheduler()
    r1 = sched.submit("alice", "req-1", [spec("t0")])
    r2 = sched.submit("alice", "req-1", [spec("t0")])
    log.info("输入: 同一 request_id=req-1 提交两次 | 判定: 第二次 DUPLICATE 且 job_id 相同")
    assert r1.status is SubmitStatus.ACCEPTED
    assert r2.status is SubmitStatus.DUPLICATE
    assert r1.job.job_id == r2.job.job_id
    assert len(sched.list_jobs()) == 1


def test_reject_reasons_are_distinct():
    sched, _ = make_scheduler(cpu=4, mem_mb=8192,
                              quotas={"bob": Resources(2, 2048)})
    cases = [
        ("bad-res", [spec("t", cpu=0)], RejectReason.INVALID_RESOURCES),
        ("too-big", [spec("t", cpu=8)], RejectReason.EXCEEDS_CAPACITY),
        ("no-dep", [spec("t")], RejectReason.DEPENDENCY_NOT_FOUND),
    ]
    for req_id, specs, expected in cases:
        deps = ["job-999"] if req_id == "no-dep" else []
        r = sched.submit("alice", req_id, specs, deps)
        log.info("输入: %s | 判定: reason=%s == %s", req_id, r.reason, expected)
        assert r.status is SubmitStatus.REJECTED
        assert r.reason is expected
    # 配额拒绝：bob 配额 2 CPU，单任务 3 CPU（不超过集群容量 4）
    r = sched.submit("bob", "over-quota", [spec("t", cpu=3)])
    log.info("输入: bob 申请 3 CPU(配额2) | 判定: reason=%s", r.reason)
    assert r.reason is RejectReason.EXCEEDS_QUOTA
    # 拒绝不产生任何作业
    assert len(sched.list_jobs()) == 0


def test_gang_scheduling_all_or_nothing():
    sched, cluster = make_scheduler(cpu=4, mem_mb=8192)
    # 作业 A：3 个任务各占 1 CPU -> 整组 3 CPU，可运行
    ra = sched.submit("alice", "a", [spec(f"t{i}") for i in range(3)])
    # 作业 B：整组需要 3 CPU，剩余 1 CPU 不够 -> 整体等待，不得部分占用
    rb = sched.submit("alice", "b", [spec(f"t{i}") for i in range(3)])
    log.info("输入: A=3x1CPU, B=3x1CPU, 容量4 | 判定: A RUNNING, B PENDING, 无部分占用")
    assert ra.job.state is JobState.RUNNING
    assert rb.job.state is JobState.PENDING
    assert all(t.state is TaskState.PENDING for t in rb.job.tasks)
    assert cluster.snapshot()["used"]["cpu"] == 3
    # A 完成后 B 整组启动
    for i in range(3):
        sched.complete_task(ra.job.job_id, i, ok=True)
    log.info("输入: A 全部完成 | 判定: B RUNNING, used=3")
    assert rb.job.state is JobState.RUNNING
    assert cluster.snapshot()["used"]["cpu"] == 3


def test_dependency_chain_and_upstream_failure():
    sched, _ = make_scheduler()
    up = sched.submit("alice", "up", [spec("t")]).job
    down = sched.submit("alice", "down", [spec("t")],
                        depends_on=[up.job_id]).job
    log.info("输入: down 依赖 up, up 失败 | 判定: down BLOCKED/upstream_failed, 未执行")
    sched.complete_task(up.job_id, 0, ok=False, error="boom")
    assert up.state is JobState.FAILED
    assert down.state is JobState.BLOCKED
    assert down.failure_reason is DependencyFailure.UPSTREAM_FAILED
    assert all(t.state is TaskState.BLOCKED for t in down.tasks)


def test_dependency_success_unblocks_downstream():
    sched, _ = make_scheduler()
    up = sched.submit("alice", "up", [spec("t")]).job
    down = sched.submit("alice", "down", [spec("t")],
                        depends_on=[up.job_id]).job
    assert down.state is JobState.PENDING  # 依赖未满足，不调度
    sched.complete_task(up.job_id, 0, ok=True)
    log.info("输入: up 成功 | 判定: down 转为 RUNNING")
    assert down.state is JobState.RUNNING


def test_cancel_blocks_downstream_and_releases():
    sched, cluster = make_scheduler(cpu=2, mem_mb=4096)
    up = sched.submit("alice", "up", [spec("t")]).job
    down = sched.submit("alice", "down", [spec("t")],
                        depends_on=[up.job_id]).job
    sched.cancel(up.job_id)
    log.info("输入: 取消运行中的 up | 判定: up CANCELLED, down BLOCKED/upstream_cancelled, used=0")
    assert up.state is JobState.CANCELLED
    assert down.state is JobState.BLOCKED
    assert down.failure_reason is DependencyFailure.UPSTREAM_CANCELLED
    assert cluster.snapshot()["used"]["cpu"] == 0


def test_task_retry_within_group_reservation():
    sched, _ = make_scheduler()
    job = sched.submit("alice", "r", [spec("t", retries=1)]).job
    assert job.state is JobState.RUNNING
    sched.complete_task(job.job_id, 0, ok=False, error="flaky")
    log.info("输入: 任务失败(可重试1次) | 判定: 任务仍 RUNNING, attempt=2, 作业未失败")
    task = job.tasks[0]
    assert task.state is TaskState.RUNNING and task.attempt == 2
    assert job.state is JobState.RUNNING
    sched.complete_task(job.job_id, 0, ok=False, error="again")
    log.info("输入: 重试后再次失败 | 判定: 任务 FAILED, 作业 FAILED")
    assert task.state is TaskState.FAILED
    assert job.state is JobState.FAILED


def test_array_partial_failure_fails_job_and_unblocks_others():
    sched, cluster = make_scheduler(cpu=4, mem_mb=8192)
    arr = sched.submit("alice", "arr", [spec(f"t{i}") for i in range(4)]).job
    waiter = sched.submit("alice", "w", [spec("t", cpu=4)]).job
    assert waiter.state is JobState.PENDING  # 容量被数组占满
    sched.complete_task(arr.job_id, 0, ok=False, error="elem0 failed")
    for i in (1, 2, 3):
        sched.complete_task(arr.job_id, i, ok=True)
    log.info("输入: 数组元素0失败其余成功 | 判定: 数组 FAILED, 资源释放, waiter RUNNING")
    assert arr.state is JobState.FAILED
    assert arr.tasks[0].state is TaskState.FAILED
    assert all(arr.tasks[i].state is TaskState.SUCCEEDED for i in (1, 2, 3))
    assert waiter.state is JobState.RUNNING
