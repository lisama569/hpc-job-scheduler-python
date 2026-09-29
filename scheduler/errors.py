"""调度器错误类型。"""


class SchedulerError(Exception):
    """调度器内部一致性错误（如重复作业号、状态机非法迁移）。"""
