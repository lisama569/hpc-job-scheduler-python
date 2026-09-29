# 本地 HPC 作业调度器

基于 FastAPI 的本地集群作业调度与资源配额服务。用本地内存状态模拟节点容量，
保证并发提交、作业依赖与取消在各条路径下结果一致且可解释。

## 资源模型

- **集群容量**：固定的 `cpu` / `mem_mb` 总量（默认 8 CPU / 16 GiB，见
  `scheduler/app.py` 中的 `default_cluster`）。
- **整组（gang）资源**：一个作业的资源需求 = 其全部任务需求之和。作业要么整组
  获得资源进入 `running`，要么整体停留在 `pending`，**绝不部分占用**。
- **用户配额**：每个用户有独立的 `cpu` / `mem_mb` 上限（未配置时使用默认配额）。
  调度时同时校验集群容量与用户配额，两者都满足才占用。
- **原子核算**：所有检查与占用在同一把锁内完成（`ClusterState.try_allocate`），
  并发提交不会超卖。

## 提交与拒绝分类

`POST /scheduler/jobs`，请求体含 `owner`、`request_id`、`tasks[]`、`depends_on[]`。

- **幂等**：同一 `request_id` 重复提交返回 `status=duplicate` 与已有作业，
  只产生一个作业。
- **拒绝**（HTTP 422，`detail.reason` 互相区分）：
  | reason | 含义 |
  |---|---|
  | `invalid_resources` | 资源参数非法（cpu/mem 非正数、任务列表为空） |
  | `exceeds_capacity` | 单任务超过集群物理容量 |
  | `exceeds_quota` | 单任务超过用户配额 |
  | `dependency_not_found` | 依赖的作业 ID 不存在 |
- 被拒绝的请求不产生任何作业，也不影响既有接口（`GET /` 保持可用）。

## 调度与依赖规则

- **依赖**：`depends_on` 中全部作业 `succeeded` 后，下游才参与调度。
- **失败归类**：上游 `failed` → 下游 `blocked`（`failure_reason=upstream_failed`）；
  上游 `cancelled` → 下游 `blocked`（`upstream_cancelled`）。下游绝不被静默执行。
- **数组任务**：任一元素失败且重试耗尽 → 作业整体 `failed`；全部元素成功 →
  `succeeded`。失败元素在 `max_retries` 额度内原地重试（资源仍被整组持有），
  `attempt` 记录当前次数。
- **取消**：`POST /scheduler/jobs/{id}/cancel` 释放整组资源，作业进入
  `cancelled`，并级联阻断下游。终态作业重复取消是幂等的。
- **释放即调度**：作业结束/取消释放资源后，调度循环立即重跑，等待中的作业
  按提交顺序整组启动。

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 既有根路由（保持不变） |
| POST | `/scheduler/jobs` | 提交作业（幂等） |
| GET | `/scheduler/jobs` | 列出作业 |
| GET | `/scheduler/jobs/{id}` | 查询作业 |
| POST | `/scheduler/jobs/{id}/complete` | 汇报任务结束 `{task_index, ok, error?}` |
| POST | `/scheduler/jobs/{id}/cancel` | 取消作业 |
| GET | `/scheduler/cluster` | 集群容量/用量快照 |

## 本地验证

```bash
uv sync
uv run python -m pytest tests/ -q
```

测试覆盖：并发超卖、重复提交去重、配额边界、依赖失败/取消的级联阻断、
数组部分失败、重试语义、取消与失败并发交错。测试日志（`log_cli` 已开启）
会打印每个用例的**输入与判定依据**，例如：

```
INFO test.concurrency: 输入: 20 线程并发提交 1CPU 作业, 容量=8 | 判定: accepted=20, running=8, pending=12, used=8 <= 8
```

手动体验：

```bash
uv run uvicorn main:app --port 8000
curl -X POST localhost:8000/scheduler/jobs -H 'content-type: application/json' -d '{
  "owner": "alice", "request_id": "demo-1",
  "tasks": [{"name": "t0", "resources": {"cpu": 2, "mem_mb": 1024}}]}'
curl localhost:8000/scheduler/cluster
```

## 代码结构

```
scheduler/
  models.py     # 作业/任务/资源模型，拒绝与失败原因枚举
  cluster.py    # 本地集群状态：容量、配额、原子占用/释放
  scheduler.py  # 调度核心：去重、校验、gang 调度、依赖推进、取消
  app.py        # FastAPI 接口层（可注入独立实例便于测试）
main.py         # 挂载子应用，保留既有根路由
tests/          # 核心语义、并发边界、HTTP 接口测试
```
