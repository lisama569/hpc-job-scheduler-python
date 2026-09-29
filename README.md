# 本地 HPC 作业调度与资源配额

基于 FastAPI 的本地集群作业调度服务：用内存状态模拟节点容量，
支持作业提交与生命周期管理、整组（gang）原子调度、作业数组与依赖链，
并保证并发提交、依赖推进与取消在各条路径下结果一致且可解释。

## 资源模型

- **集群容量**：由 `ClusterCapacity(total_cpu, total_mem_mb)` 定义，
  本地用一本集中式资源账本（`scheduler/cluster.py`）模拟节点容量。
  所有"检查 + 扣减"都在同一把锁内原子完成，因此并发提交不会超卖。
- **资源申请**：每个作业声明单任务资源 `ResourceRequest(cpu, mem_mb)`
  与任务数 `num_tasks`，整组总量 = 单任务资源 × 任务数。
- **用户配额**：每个用户被接纳（PENDING/RUNNING）作业的整组资源之和
  不得超过配额；超出即在提交时拒绝（`QUOTA_EXCEEDED`）。
- 容量与配额可用环境变量覆盖：`CLUSTER_CPU`、`CLUSTER_MEM_MB`、
  `QUOTA_CPU`、`QUOTA_MEM_MB`（默认 16 CPU / 16384MB，配额等于集群容量）。

## 提交规则与拒绝分类

提交即校验，各类拒绝原因通过 `SubmissionError.reason`（HTTP 响应的
`detail.reason`）互相区分：

| 原因 | HTTP | 含义 |
| --- | --- | --- |
| `INVALID_RESOURCES` | 400 | 资源参数非法：非正数、超集群容量，或整组总量永远放不下 |
| `UNKNOWN_DEPENDENCY` | 404 | 依赖的作业 ID 不存在 |
| `QUOTA_EXCEEDED` | 409 | 用户已占资源 + 本次申请超出配额 |
| `DUPLICATE_REQUEST` | 409 | 相同 `request_id` 但参数不一致 |

**幂等提交**：`request_id` 是幂等键。同一 `request_id` 以相同参数重复提交，
返回已存在的作业（`created=false`），只产生一个作业；参数不一致则拒绝。

## 调度规则

- **整组原子调度（gang）**：多任务作业一次性申请全部任务的资源总量，
  要么整体进入 RUNNING，要么整体留在 PENDING，不存在部分任务先占用资源。
- **确定性推进**：每次状态变化（提交、任务结束、取消）后按提交顺序扫描
  PENDING 作业：依赖未就绪或资源不足的跳过继续等待（允许小作业回填），
  同一事件序列必然得到同一结果。
- **释放即回填**：作业进入终态立即释放资源并触发新一轮推进。

## 依赖与数组语义

- **依赖链**：`depends_on` 中的作业全部 SUCCEEDED 后下游才可被调度；
  上游 FAILED → 下游直接判 FAILED（`DEPENDENCY_FAILED`）；
  上游 CANCELLED → 下游判 CANCELLED（`DEPENDENCY_CANCELLED`），
  并沿依赖链级联。下游不会被静默执行。
- **失败归类**：`failure_kind` 区分 `TASK_FAILED`（任务自身失败且重试耗尽）、
  `DEPENDENCY_FAILED`、`DEPENDENCY_CANCELLED`，`failure_reason` 携带可追溯详情。
- **数组重试**：`max_retries` 控制每个元素的重试次数。元素失败且额度未尽时
  原地重跑（资源仍由整组持有，`attempt` 递增）；额度耗尽则整个作业 FAILED，
  同组其余运行中元素一并取消并释放资源。
- **取消**：PENDING/RUNNING 作业可取消，释放资源并级联处理下游；
  对终态作业取消是幂等空操作。

## 对外接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 健康检查 |
| POST | `/jobs` | 提交作业（幂等） |
| GET | `/jobs` / `/jobs/{id}` | 查询作业 |
| POST | `/jobs/{id}/cancel` | 取消作业 |
| POST | `/jobs/{id}/tasks/{i}/succeed` | 模拟数组元素成功 |
| POST | `/jobs/{id}/tasks/{i}/fail` | 模拟数组元素失败（可带 `reason`） |
| GET | `/cluster/usage` | 集群资源快照 |

## 本地验证

```bash
# 安装依赖
uv sync

# 运行全部测试（日志会打印每个用例的输入与判定依据）
uv run pytest

# 并发与资源边界用例可重复运行验证结论稳定
for i in 1 2 3 4 5; do uv run pytest tests/test_concurrency.py -q; done

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app --port 8000
```

手工验证示例：

```bash
# 提交一个 4 任务数组作业
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' -d \
  '{"name":"demo","user":"alice","request_id":"req-1","num_tasks":4,"cpu":1}'

# 重复提交同一 request_id -> 返回同一作业, created=false
curl -s -X POST localhost:8000/jobs -H 'content-type: application/json' -d \
  '{"name":"demo","user":"alice","request_id":"req-1","num_tasks":4,"cpu":1}'

# 查看资源占用
curl -s localhost:8000/cluster/usage
```

## 代码结构

```
scheduler/
  models.py    # 资源/作业/任务模型，状态与失败归类枚举
  errors.py    # 提交拒绝原因分类与异常
  cluster.py   # 本地集群资源账本（原子扣减，不超卖）
  engine.py    # 调度引擎：校验、幂等、配额、整组调度、依赖推进
main.py        # FastAPI 对外接口（参数转换与错误映射）
tests/         # 提交、并发边界、依赖/数组、接口四层测试
```
