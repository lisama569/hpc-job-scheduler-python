"""依赖链与作业数组：推进、失败归类、取消级联、重试语义。"""

from __future__ import annotations

import logging

from scheduler import (
    FailureKind,
    JobSpec,
    JobState,
    ResourceRequest,
    TaskState,
)

log = logging.getLogger("test.dependencies")


def _spec(request_id: str, **kw) -> JobSpec:
    args = dict(name="job", user="alice", request_id=request_id)
    args.update(kw)
    return JobSpec(**args)


def test_dependency_chain_promotes_in_order(make_scheduler):
    s = make_scheduler()
    a, _ = s.submit(_spec("a"))
    b, _ = s.submit(_spec("b", depends_on=(a.job_id,)))
    c, _ = s.submit(_spec("c", depends_on=(b.job_id,)))
    log.info("输入: 链式依赖 a <- b <- c, 依次成功")

    assert b.state is JobState.PENDING and c.state is JobState.PENDING
    s.task_succeed(a.job_id, 0)
    log.info("判定依据: a 成功后 b=%s c=%s", b.state.value, c.state.value)
    assert b.state is JobState.RUNNING and c.state is JobState.PENDING
    s.task_succeed(b.job_id, 0)
    assert c.state is JobState.RUNNING
    s.task_succeed(c.job_id, 0)
    assert c.state is JobState.SUCCEEDED


def test_upstream_failure_blocks_downstream_with_classification(make_scheduler):
    s = make_scheduler()
    a, _ = s.submit(_spec("a"))
    b, _ = s.submit(_spec("b", depends_on=(a.job_id,)))
    c, _ = s.submit(_spec("c", depends_on=(b.job_id,)))
    log.info("输入: 链 a <- b <- c, 使 a 的唯一任务失败(无重试)")

    s.task_fail(a.job_id, 0, "oom")

    log.info("判定依据: a=%s(%s) b=%s(%s) c=%s(%s)",
             a.state.value, a.failure_kind, b.state.value, b.failure_kind,
             c.state.value, c.failure_kind)
    assert a.state is JobState.FAILED and a.failure_kind is FailureKind.TASK_FAILED
    # 下游不得静默执行：直接判 FAILED 且归类为 DEPENDENCY_FAILED，级联到 c
    assert b.state is JobState.FAILED and b.failure_kind is FailureKind.DEPENDENCY_FAILED
    assert c.state is JobState.FAILED and c.failure_kind is FailureKind.DEPENDENCY_FAILED
    assert "oom" in (b.failure_reason or "")       # 失败原因可追溯
    assert s.usage().used_cpu == 0                 # 资源全部释放


def test_upstream_cancel_cancels_downstream(make_scheduler):
    s = make_scheduler()
    a, _ = s.submit(_spec("a"))
    b, _ = s.submit(_spec("b", depends_on=(a.job_id,)))
    log.info("输入: a <- b, 取消运行中的 a")

    s.cancel(a.job_id)

    log.info("判定依据: a=%s b=%s(%s)", a.state.value, b.state.value, b.failure_kind)
    assert a.state is JobState.CANCELLED
    assert b.state is JobState.CANCELLED
    assert b.failure_kind is FailureKind.DEPENDENCY_CANCELLED
    assert s.usage().used_cpu == 0


def test_cancel_is_idempotent_on_terminal_job(make_scheduler):
    s = make_scheduler()
    a, _ = s.submit(_spec("a"))
    s.task_succeed(a.job_id, 0)
    log.info("输入: 对已成功作业再次取消")
    s.cancel(a.job_id)
    log.info("判定依据: 终态不变, state=%s", a.state.value)
    assert a.state is JobState.SUCCEEDED


def test_array_retry_then_succeed(make_scheduler):
    s = make_scheduler()
    j, _ = s.submit(_spec("arr", resources=ResourceRequest(cpu=1, mem_mb=64),
                          num_tasks=3, max_retries=1))
    log.info("输入: 3 元素数组, max_retries=1, 元素 1 先失败一次再成功")

    s.task_fail(j.job_id, 1, "flaky")
    t1 = j.tasks[1]
    log.info("判定依据: 第一次失败后 元素1 state=%s attempt=%d, 作业=%s",
             t1.state.value, t1.attempt, j.state.value)
    assert t1.state is TaskState.RUNNING and t1.attempt == 2   # 原地重试
    assert j.state is JobState.RUNNING

    s.task_succeed(j.job_id, 0)
    s.task_succeed(j.job_id, 1)
    s.task_succeed(j.job_id, 2)
    assert j.state is JobState.SUCCEEDED


def test_array_retry_exhausted_fails_whole_job(make_scheduler):
    s = make_scheduler()
    j, _ = s.submit(_spec("arr", resources=ResourceRequest(cpu=1, mem_mb=64),
                          num_tasks=3, max_retries=1))
    log.info("输入: 3 元素数组, max_retries=1, 元素 0 连续失败两次")

    s.task_fail(j.job_id, 0, "err-1")
    s.task_fail(j.job_id, 0, "err-2")

    log.info("判定依据: 作业=%s kind=%s, 任务状态=%s",
             j.state.value, j.failure_kind, [t.state.value for t in j.tasks])
    assert j.state is JobState.FAILED
    assert j.failure_kind is FailureKind.TASK_FAILED
    assert j.tasks[0].state is TaskState.FAILED
    # 同组其余运行中任务随作业终止，不得继续占用资源
    assert all(t.state is TaskState.CANCELLED for t in j.tasks[1:])
    assert s.usage().used_cpu == 0


def test_cancel_pending_gang_releases_nothing_and_stays_consistent(make_scheduler):
    # 集群 2cpu：先占满，再提交一个 2cpu 整组 -> 等待；取消等待中的整组
    s = make_scheduler(cpu=2, mem_mb=256)
    j1, _ = s.submit(_spec("filler", resources=ResourceRequest(cpu=2, mem_mb=128)))
    g, _ = s.submit(_spec("gang", resources=ResourceRequest(cpu=1, mem_mb=64), num_tasks=2))
    log.info("输入: 集群 2cpu 已被占满, 提交 2x1cpu 整组后取消它")
    assert g.state is JobState.PENDING

    s.cancel(g.job_id)

    log.info("判定依据: gang=%s, 任务=%s, used_cpu=%d",
             g.state.value, [t.state.value for t in g.tasks], s.usage().used_cpu)
    assert g.state is JobState.CANCELLED
    assert all(t.state is TaskState.CANCELLED for t in g.tasks)
    assert s.usage().used_cpu == 2                 # 只释放自己(本就没占)，filler 不受影响
    assert j1.state is JobState.RUNNING
