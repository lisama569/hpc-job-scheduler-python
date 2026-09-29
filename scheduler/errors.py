"""提交拒绝原因与异常定义（骨架）。"""

from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    """提交被拒绝的分类原因，彼此可区分。"""

    INVALID_RESOURCES = "INVALID_RESOURCES"      # 资源参数非法（非正数、超单机容量等）
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"            # 超出用户配额
    UNKNOWN_DEPENDENCY = "UNKNOWN_DEPENDENCY"    # 依赖的作业不存在
    DUPLICATE_REQUEST = "DUPLICATE_REQUEST"      # 相同 request_id 但参数不一致


class SubmissionError(Exception):
    """作业提交被拒绝时抛出，携带分类原因。"""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")
