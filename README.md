# Cherry Sync Server

Cherry Studio 增量同步服务端。

## 启动

```bash
# 设置环境变量（可选，有默认值）
export SYNC_TOKEN="your-secret-token"
export PORT=3456
export SYNC_BODY_LIMIT=500mb


node server.js
```

## 反向代理与 413（Request Entity Too Large）

如果 Cherry Studio 日志出现：

- `POST /api/topics/batch failed: 413`
- 响应体含 `nginx/1.24.0` 或 `Request Entity Too Large`

通常是 Nginx 的请求体限制过小（默认约 1MB），需要在对应 `server` 或 `location` 中调大：

```nginx
server {
    listen 2053 ssl;
    server_name your.domain.com;

    client_max_body_size 500m; # 建议 >= Node 端 JSON limit

    location / {
        proxy_pass http://127.0.0.1:3456;
    }
}
```

修改后执行：

```bash
nginx -t
systemctl reload nginx
```

注意：

- 本服务默认 `express.json` 限制为 `500mb`，可通过 `SYNC_BODY_LIMIT` 调整。
- 建议 Nginx `client_max_body_size` 与其保持一致或更高（例如 `500m`），避免代理层先拦截。

## API

所有 `/api/*` 请求需要 `Authorization: Bearer <token>` 头。

| 端点                                                            | 方法   | 说明                                             |
| --------------------------------------------------------------- | ------ | ------------------------------------------------ |
| `GET /health`                                                   | -      | 健康检查（无需认证）                             |
| `GET /api/topics`                                               | GET    | 列出所有 Topic（不含消息体）                     |
| `GET /api/topics/:id`                                           | GET    | 获取完整 Topic 数据                              |
| `GET /api/topics/:id/revision`                                  | GET    | 获取 Topic 版本元数据（`seq/revision`）          |
| `POST /api/topics`                                              | POST   | 上传/更新单个 Topic（返回 `applied/noop/stale/conflict`） |
| `POST /api/topics/batch`                                        | POST   | 批量上传 Topics（逐条回执）                      |
| `DELETE /api/topics/:id`                                        | DELETE | 软删除 Topic（返回逐条状态）                     |
| `POST /api/topics/delete-batch`                                 | POST   | 批量删除 Topic（逐条回执）                       |
| `GET /api/sync/changes?cursor=<seq>&limit=<n>&includePayload=1` | GET    | 基于 `seq` 的分页增量变更（推荐）                |
| `GET /api/sync?since=<cursor>`                                  | GET    | 旧接口（仅返回 upserts/deletes id 列表）         |

## 同步语义

- 服务器使用单调 `seq` 作为增量游标，不再用时间戳做游标。
- 每个 Topic 维护 `revision` 与 `client_updated_at`。
- 对同一 Topic 的冲突处理规则：
  - 更旧的 `client_updated_at` 会返回 `stale`（不会覆盖新版本）。
  - 相同内容会返回 `noop`（幂等）。
  - 其余更新返回 `applied`。

## 冲突控制（新增）

- 写入接口支持乐观锁（CAS）：
  - `X-Sync-If-Revision: <number>`（请求头）
  - 或在 JSON 中传 `expectedRevision` / `syncMeta.expectedRevision`
- 当期望版本与服务端 `revision` 不一致时，返回 `status: "conflict"`，并携带服务端当前版本元数据。

## 强制写入（用于 local_wins 回写）

- 写入接口支持 `X-Sync-Force: 1`（请求头）强制覆盖版本检查与时间戳 stale 保护。
- 也支持在 JSON 中传 `force` / `syncMeta.force`。
- 服务端会自动剥离 `syncMeta/force/expectedRevision`，不会写入 Topic 正文数据，避免污染消息内容。

## 历史数据修复：回填 `askId`

如果旧快照里的 assistant 消息缺少 `askId`，或 `askId` 不是对应 user message 的 `id`，可以离线修复 SQLite：

```bash
cd cherry-sync-server
npm run repair:askid -- --assistant-name Gemini --assistant-name ChatGPT --assistant-name "ChatGPT 导入" --assistant-name POE
```

先执行不带 `--apply` 的 dry-run，确认扫描结果；确认后再真正写入：

```bash
cd cherry-sync-server
npm run repair:askid -- --assistant-name Gemini --assistant-name ChatGPT --assistant-name "ChatGPT 导入" --assistant-name POE --apply
```

说明：

- assistant 的 `askId` 会被回填为“最近一条 user message 的 `id`”
- user 消息上错误残留的 `askId` 会被清掉
- 每个被修复的 topic 都会推进 `revision/seq`，方便客户端下次同步时识别成服务端更新

## 一次性彻底升级旧 `sync.db`

如果你已经不需要兼容旧客户端，可以直接把旧库整体升级到当前同步模型：

```bash
cd cherry-sync-server
npm run upgrade:db
```

确认 dry-run 后，执行真正升级：

```bash
cd cherry-sync-server
npm run upgrade:db -- --apply
```

升级内容：

- 确保 `topics/meta` 表和 `seq/revision/client_updated_at/content_hash` 列存在
- 规范化每条 topic 快照里的 `topicId/name/assistantId/assistantName/createdAt/updatedAt/messages`
- 修复 assistant `askId`
- 为所有 topic 重新建立 `seq` 和 `revision=1` 的新基线
- 重建 `meta.change_seq`
- 默认自动备份原数据库
