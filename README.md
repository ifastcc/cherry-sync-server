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
| `POST /api/topics` | POST | 上传/更新单个 Topic |
| `POST /api/topics/batch` | POST | 批量上传 Topics |
| `DELETE /api/topics/:id` | DELETE | 软删除 Topic |
| `GET /api/sync?since=<ts>` | GET | 增量变更查询 |
