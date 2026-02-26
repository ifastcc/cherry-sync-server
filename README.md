# Cherry Sync Server

Cherry Studio 增量同步服务端。

## 启动

```bash
# 设置环境变量（可选，有默认值）
export SYNC_TOKEN="your-secret-token"
export PORT=3456

# 启动
node server.js
```

## API

所有 `/api/*` 请求需要 `Authorization: Bearer <token>` 头。

| 端点 | 方法 | 说明 |
|---|---|---|
| `GET /health` | - | 健康检查（无需认证） |
| `GET /api/topics` | GET | 列出所有 Topic（不含消息体） |
| `GET /api/topics/:id` | GET | 获取完整 Topic 数据 |
| `POST /api/topics` | POST | 上传/更新单个 Topic（返回 `applied/noop/stale`） |
| `POST /api/topics/batch` | POST | 批量上传 Topics（逐条回执） |
| `DELETE /api/topics/:id` | DELETE | 软删除 Topic（返回逐条状态） |
| `POST /api/topics/delete-batch` | POST | 批量删除 Topic（逐条回执） |
| `GET /api/sync/changes?cursor=<seq>&limit=<n>&includePayload=1` | GET | 基于 `seq` 的分页增量变更（推荐） |
| `GET /api/sync?since=<cursor>` | GET | 旧接口（仅返回 upserts/deletes id 列表） |

## 同步语义

- 服务器使用单调 `seq` 作为增量游标，不再用时间戳做游标。
- 每个 Topic 维护 `revision` 与 `client_updated_at`。
- 对同一 Topic 的冲突处理规则：
  - 更旧的 `client_updated_at` 会返回 `stale`（不会覆盖新版本）。
  - 相同内容会返回 `noop`（幂等）。
  - 其余更新返回 `applied`。
