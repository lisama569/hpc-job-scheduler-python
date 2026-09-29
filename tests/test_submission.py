"""提交路径：幂等、参数校验、配额、依赖存在性。

每个用例都打印输入与判定依据，结论与事件顺序无关、可稳定复现。
"""

from __future__ import annotations

import logging

import pytest

from scheduler import JobSpec, JobState, ResourceRequest
from scheduler.errors import RejectReason, SubmissionError

log = logging.getLogger("test.submission")


def _spec(request_id: str, **kw) -> JobSpec:
    args = dict(name="job", user="alice", request_id=request_id)
    args.update(kw)
    return JobSpec(**args)


def test_duplicate_submission_is_idempotent(make_scheduler):
    s = make_scheduler()
    spec = _spec("req-1", resources=ResourceRequest(cpu=2, mem_mb=128), num_tasks=2)
    log.info("输入: 同一 request_id=req-1 连续提交两次, spec=%s", spec)

    j1, created1 = s.submit(spec)
    j2, created2 = s.submit(spec)

    log.info("判定依据: 第一次 created=%s, 第二次 created=%s, job_id 应相同", created1, created2)
    assert created1 is True
    assert created2 is False
    assert j1.job_id == j2.job_id
    assert len(s.list_jobs()) == 1


def test_duplicate_request_with_conflicting_params_rejected(make_scheduler):
    s = make_scheduler()
    s.submit(_spec("req-1", resources=ResourceRequest(cpu=1, mem_mb=128)))
    log.info("输入: request_id=req-1 已存在, 再以不同资源参数(cpu=2)提交")

    with pytest.raises(SubmissionError) as ei:
        s.submit(_spec("req-1", resources=ResourceRequest(cpu=2, mem_mb=128)))

    log.info("判定依据: 拒绝原因应为 DUPLICATE_REQUEST, 实际=%s", ei.value.reason)
    assert ei.value.reason is RejectReason.DUPLICATE_REQUEST


@pytest.mark.parametrize(
    "resources,num_tasks,why",
    [
        (ResourceRequest(cpu=0, mem_mb=128), 1, "cpu 为 0 非法"),
        (ResourceRequest(cpu=1, mem_mb=-1), 1, "mem 为负非法"),
        (ResourceRequest(cpu=1, mem_mb=128), 0, "num_tasks 为 0 非法"),
        (ResourceRequest(cpu=100, mem_mb=128), 1, "单任务超出集群容量"),
        (ResourceRequest(cpu=3, mem_mb=128), 4, "整组 12cpu 超出 8cpu 集群容量"),
    ],
)
def test_invalid_resources_rejected(make_scheduler, resources, num_tasks, why):
    s = make_scheduler()
    log.info("输入: resources=%s num_tasks=%s (%s)", resources, num_tasks, why)

    with pytest.raises(SubmissionError) as ei:
        s.submit(_spec(f"bad-{why}", resources=resources, num_tasks=num_tasks))

    log.info("判定依据: 拒绝原因应为 INVALID_RESOURCES, 实际=%s, 详情=%s",
             ei.value.reason, ei.value.detail)
    assert ei.value.reason is RejectReason.INVALID_RESOURCES
    assert len(s.list_jobs()) == 0


def test_quota_exceeded_rejected_and_distinguishable(make_scheduler):
    # 集群 8cpu，配额 6cpu：先占 4，再申请 3 -> 4+3=7 > 6
    s = make_scheduler(quota_cpu=6, quota_mem_mb=1024 * 4)
    j1, _ = s.submit(_spec("q-1", resources=ResourceRequest(cpu=4, mem_mb=128)))
    assert j1.state is JobState.RUNNING
    log.info("输入: 配额 6cpu, 已占 4cpu, 再申请 3cpu 的作业")

    with pytest.raises(SubmissionError) as ei:
        s.submit(_spec("q-2", resources=ResourceRequest(cpu=3, mem_mb=128)))

    log.info("判定依据: 拒绝原因应为 QUOTA_EXCEEDED(而非资源非法), 实际=%s, 详情=%s",
             ei.value.reason, ei.value.detail)
    assert ei.value.reason is RejectReason.QUOTA_EXCEEDED
    # 被拒绝不影响已有作业
    assert s.get_job(j1.job_id).state is JobState.RUNNING


def test_unknown_dependency_rejected(make_scheduler):
    s = make_scheduler()
    log.info("输入: 提交依赖不存在作业 job-404 的作业")

    with pytest.raises(SubmissionError) as ei:
        s.submit(_spec("dep-1", depends_on=("job-404",)))

    log.info("判定依据: 拒绝原因应为 UNKNOWN_DEPENDENCY, 实际=%s", ei.value.reason)
    assert ei.value.reason is RejectReason.UNKNOWN_DEPENDENCY
    assert len(s.list_jobs()) == 0
