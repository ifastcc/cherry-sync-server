const express = require('express')
const Database = require('better-sqlite3')
const cors = require('cors')
const path = require('path')
const fs = require('fs')
const util = require('util')

// ── 配置 ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3456
const TOKEN = process.env.SYNC_TOKEN || 'cherry-sync-default-token'
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data', 'sync.db')
const LOG_PATH = path.join(path.dirname(DB_PATH), 'sync.log')

// ── 数据库与日志设置 ──────────────────────────────────────────────────
fs.mkdirSync(path.dirname(DB_PATH), { recursive: true })

const logFile = fs.createWriteStream(LOG_PATH, { flags: 'a' })
const getTimestamp = () => new Date().toISOString()

const originalLog = console.log
console.log = (...args) => {
  const output = util.format(...args)
  const msg = `[${getTimestamp()}] ${output}`
  originalLog(msg)
  logFile.write(msg + '\n')
}

const originalError = console.error
console.error = (...args) => {
  const output = util.format(...args)
  const msg = `[${getTimestamp()}] [ERR] ${output}`
  originalError(msg)
  logFile.write(msg + '\n')
}

const db = new Database(DB_PATH)
db.pragma('journal_mode = WAL')
db.pragma('foreign_keys = ON')

db.exec(`
  CREATE TABLE IF NOT EXISTS topics (
    topic_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    assistant_id TEXT,
    assistant_name TEXT,
    data        TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    deleted_at  INTEGER
  );
  CREATE INDEX IF NOT EXISTS idx_topics_updated ON topics(updated_at);
  CREATE INDEX IF NOT EXISTS idx_topics_deleted ON topics(deleted_at);
`)

// ── Prepared Statements ───────────────────────────────────────────────
const stmts = {
  upsert: db.prepare(`
    INSERT INTO topics (topic_id, name, assistant_id, assistant_name, data, created_at, updated_at, deleted_at)
    VALUES (@topic_id, @name, @assistant_id, @assistant_name, @data, @created_at, @updated_at, NULL)
    ON CONFLICT(topic_id) DO UPDATE SET
      name = @name,
      assistant_id = @assistant_id,
      assistant_name = @assistant_name,
      data = @data,
      updated_at = @updated_at,
      deleted_at = NULL
  `),
  softDelete: db.prepare(`
    UPDATE topics SET deleted_at = @now, updated_at = @now WHERE topic_id = @topic_id
  `),
  getOne: db.prepare(`
    SELECT topic_id, name, assistant_id, assistant_name, data, created_at, updated_at
    FROM topics WHERE topic_id = ? AND deleted_at IS NULL
  `),
  listAll: db.prepare(`
    SELECT topic_id, name, assistant_id, assistant_name, created_at, updated_at
    FROM topics WHERE deleted_at IS NULL ORDER BY updated_at DESC
  `),
  changesSince: db.prepare(`
    SELECT topic_id,
      CASE WHEN deleted_at IS NOT NULL THEN 'delete' ELSE 'upsert' END AS op,
      updated_at
    FROM topics
    WHERE updated_at > @since OR (deleted_at IS NOT NULL AND deleted_at > @since)
    ORDER BY updated_at ASC
  `),
}

// ── Express ───────────────────────────────────────────────────────────
const app = express()
app.use(cors())
app.use(express.json({ limit: '50mb' }))

// 请求日志拦截器
app.use((req, res, next) => {
  if (req.path.startsWith('/api/') || req.path === '/health') {
    const start = Date.now()
    res.on('finish', () => {
      const duration = Date.now() - start
      console.log(`[HTTP] ${req.method} ${req.url} ${res.statusCode} - ${duration}ms`)
    })
  }
  next()
})

// Token 认证（简单 Bearer token）
app.use('/api', (req, res, next) => {
  const auth = req.headers.authorization
  if (!auth || auth !== `Bearer ${TOKEN}`) {
    return res.status(401).json({ error: 'Unauthorized' })
  }
  next()
})

// ── GET /api/topics ── 列出所有 Topic（不含 data，轻量）─────────────
app.get('/api/topics', (req, res) => {
  try {
    const topics = stmts.listAll.all()
    res.json({ topics })
  } catch (e) {
    console.error('[GET /api/topics]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── GET /api/topics/:id ── 获取单个 Topic 完整数据 ──────────────────
app.get('/api/topics/:id', (req, res) => {
  try {
    const row = stmts.getOne.get(req.params.id)
    if (!row) return res.status(404).json({ error: 'Topic not found' })
    res.json({ ...row, data: JSON.parse(row.data) })
  } catch (e) {
    console.error('[GET /api/topics/:id]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── POST /api/topics ── 上传/更新 Topic ─────────────────────────────
app.post('/api/topics', (req, res) => {
  try {
    const body = req.body
    if (!body.topicId) return res.status(400).json({ error: 'topicId is required' })

    const now = Date.now()
    stmts.upsert.run({
      topic_id: body.topicId,
      name: body.name || '',
      assistant_id: body.assistantId || null,
      assistant_name: body.assistantName || null,
      data: JSON.stringify(body),
      created_at: body.createdAt ? new Date(body.createdAt).getTime() : now,
      updated_at: body.updatedAt ? new Date(body.updatedAt).getTime() : now,
    })

    console.log(`[SYNC] Upserted Topic: ${body.topicId} (${body.name || '未命名'})`)
    res.json({ ok: true, topicId: body.topicId })
  } catch (e) {
    console.error('[POST /api/topics]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── POST /api/topics/batch ── 批量上传 Topics ───────────────────────
const batchUpsert = db.transaction((topics) => {
  const now = Date.now()
  let count = 0
  for (const body of topics) {
    if (!body.topicId) continue
    stmts.upsert.run({
      topic_id: body.topicId,
      name: body.name || '',
      assistant_id: body.assistantId || null,
      assistant_name: body.assistantName || null,
      data: JSON.stringify(body),
      created_at: body.createdAt ? new Date(body.createdAt).getTime() : now,
      updated_at: body.updatedAt ? new Date(body.updatedAt).getTime() : now,
    })
    count++
  }
  return count
})

app.post('/api/topics/batch', (req, res) => {
  try {
    const { topics } = req.body
    if (!Array.isArray(topics)) return res.status(400).json({ error: 'topics array is required' })
    const count = batchUpsert(topics)
    console.log(`[SYNC] Batch Upserted: ${count} topics`)
    res.json({ ok: true, count })
  } catch (e) {
    console.error('[POST /api/topics/batch]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── DELETE /api/topics/:id ── 软删除 Topic ──────────────────────────
app.delete('/api/topics/:id', (req, res) => {
  try {
    stmts.softDelete.run({ topic_id: req.params.id, now: Date.now() })
    console.log(`[SYNC] Deleted Topic: ${req.params.id}`)
    res.json({ ok: true })
  } catch (e) {
    console.error('[DELETE /api/topics/:id]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── GET /api/sync?since=<timestamp> ── 增量变更查询 ─────────────────
app.get('/api/sync', (req, res) => {
  try {
    const since = parseInt(req.query.since) || 0
    const changes = stmts.changesSince.all({ since })

    const upserts = []
    const deletes = []
    for (const c of changes) {
      if (c.op === 'delete') deletes.push(c.topic_id)
      else upserts.push(c.topic_id)
    }

    res.json({
      serverTime: Date.now(),
      since,
      upserts,
      deletes,
      totalChanges: changes.length,
    })
  } catch (e) {
    console.error('[GET /api/sync]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── 健康检查 ────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const count = db.prepare('SELECT COUNT(*) as count FROM topics WHERE deleted_at IS NULL').get()
  res.json({ status: 'ok', topics: count.count, uptime: process.uptime() })
})

// ── 启动 ────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  const count = db.prepare('SELECT COUNT(*) as count FROM topics WHERE deleted_at IS NULL').get()
  console.log(`🍒 Cherry Sync Server listening on http://localhost:${PORT}`)
  console.log(`   Database: ${DB_PATH}`)
  console.log(`   Topics: ${count.count}`)
  console.log(`   Token: ${TOKEN.slice(0, 8)}...`)
})

process.on('SIGINT', () => {
  db.close()
  process.exit(0)
})
