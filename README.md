# 随访明细交付平台

队列数仓数据管理员向研究组交付随访明细的单实例网页应用（FastAPI + SQLite）。

- **导入**：上传三列 CSV（`participant_id, visit_date, score`），新参与者初始均已获授权
- **交付**：勾选一组参与者生成批次 CSV，保留其全部随访行；批次取**创建时**已授权的所选人员，选到已撤回人员则拒绝创建（409）
- **撤回**：管理员可**永久撤回**某参与者的授权。撤回会：
  - 中止包含此人的**生成作业**（清理半成品文件）与**在途下载**（已发出的字节无需追回）
  - 将涉及批次**永久封锁**，禁止再次下载；其他批次照常
  - 等相关作业和下载**都停止后**才返回撤回成功；慢下载不会卡住不相关批次
- **持久化**：SQLite 元数据、批次文件、涉及人员、撤回记录全部落在数据卷。重启后被封锁批次仍禁止下载，未完成的生成批次标记失败；生成或保存失败的批次不提供下载

## 启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

## 网页试用流程

1. 点击 **导入内置示例数据**（80 名参与者 × 18 次访视，也可下载示例 CSV 后用文件上传）
2. **全选** → **用选中参与者创建批次**，等状态变为「可下载」
3. 演示**慢下载期间撤回授权**：
   - 点批次行的 **慢速下载**（人为节流约 10 秒，模拟大文件慢链路）
   - 下载进行中，在参与者列表对批次内任意一人点 **永久撤回**
   - 观察：浏览器下载立即中断失败；撤回接口在该下载停止后才返回成功；批次状态变「已封锁」，下载链接消失，再访问下载地址返回 403
   - 再建一个不含此人的批次，其生成与下载不受任何影响
4. 演示**生成中撤回**：创建批次后 1~2 秒内撤回其中某人，生成作业被中止，批次直接「已封锁」
5. 演示**重启持久化**：`docker compose restart`（或生成中直接 `docker compose kill` 再 `up`），被封锁批次仍 403，生成中的批次变为「已失败」

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/import` | 上传 CSV 导入（multipart 文件字段 `file`） |
| POST | `/api/import-sample` | 导入内置示例数据 |
| GET  | `/api/participants` | 参与者列表（授权状态、随访行数） |
| POST | `/api/batches` | 创建批次，body: `{"participant_ids": [...]}`；含已撤回者返回 409 |
| GET  | `/api/batches` | 批次列表（状态、涉及人员、错误信息） |
| GET  | `/api/batches/{id}/download?slow=1` | 下载批次 CSV；`slow=1` 人为节流用于演示；blocked→403，非 ready→409 |
| POST | `/api/participants/{pid}/revoke` | 永久撤回授权；中止相关作业/下载并等待其停止后返回 |

## 并发设计

批次状态机：`generating → ready / failed / blocked`，`ready → blocked`，`blocked` 与 `failed` 为终态。

- 所有「检查状态 + 变更状态」的临界区（创建批次、开始下载并登记、生成完成落盘、撤回置 blocked）共用一把 asyncio 锁，因此**生成完成、开始下载、撤回**三者并发时只会按某一种顺序生效：
  - 下载先登记 → 撤回会发现并中止它，且等它停止后才返回
  - 撤回先封锁 → 下载请求直接被 403 拒绝
  - 生成先完成 → 撤回随后封锁批次并清理在途下载
  - 撤回先封锁 → 生成作业丢弃半成品文件，不会置为 ready
- 撤回只等待**受影响**的作业与下载，无关批次的慢下载照常进行
- 重启恢复：启动时把 `generating` 批次标记为 `failed`，文件缺失的 `ready` 批次同样置 `failed`

## 数据卷

容器内 `/data`（compose 命名卷 `followup_data`）：

```
/data/app.db                 # SQLite：参与者、授权、批次、涉及人员、撤回记录
/data/batches/batch_<id>.csv # 交付文件（生成失败/被中止时不留文件）
```

## 本地开发（不用 Docker）

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --port 8000
```

可用环境变量：`DATA_DIR`、`GEN_CHUNK_ROWS`、`GEN_CHUNK_DELAY_MS`（生成节流）、
`DL_CHUNK_BYTES`、`DL_CHUNK_DELAY_MS`（常规下载节流）、
`SLOW_CHUNK_BYTES`、`SLOW_CHUNK_DELAY_MS`（慢速下载节流）。
