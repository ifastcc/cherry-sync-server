const express = require('express')
const Database = require('better-sqlite3')
const cors = require('cors')
const path = require('path')
const fs = require('fs')
const util = require('util')
const crypto = require('crypto')

// ── 配置 ──────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3456
const TOKEN = process.env.SYNC_TOKEN || 'cherry-sync-default-token'
const CORS_ORIGINS = process.env.CORS_ORIGINS || '' // 逗号分隔的白名单，留空则允许所有来源
const DB_PATH = process.env.DB_PATH || path.join(__dirname, 'data', 'sync.db')
const LOG_PATH = path.join(path.dirname(DB_PATH), 'sync.log')

const DEFAULT_PAGE_LIMIT = 200
const MAX_PAGE_LIMIT = 1000

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

function parseClientTimestamp(value) {
  if (value == null) return null
  const ts = typeof value === 'number' ? value : new Date(value).getTime()
  if (!Number.isFinite(ts) || ts <= 0) return null
  return Math.floor(ts)
}

function sha1Hex(value) {
  return crypto.createHash('sha1').update(value).digest('hex')
}

function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value, 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback
}

function parseOptionalPositiveInt(value) {
  if (value == null || value === '') return null
  const parsed = Number.parseInt(String(value), 10)
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null
}

function parseBoolean(value, fallback = false) {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  if (typeof value !== 'string') return fallback
  const normalized = value.trim().toLowerCase()
  if (normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on') return true
  if (normalized === '0' || normalized === 'false' || normalized === 'no' || normalized === 'off') return false
  return fallback
}

function isRecord(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function headerFirst(value) {
  if (Array.isArray(value)) return value[0]
  return value
}

function clamp(value, min, max) {
  if (value < min) return min
  if (value > max) return max
  return value
}

function ensureTopicColumn(columnName, definition) {
  const columns = db.prepare('PRAGMA table_info(topics)').all().map((item) => item.name)
  if (!columns.includes(columnName)) {
    db.exec(`ALTER TABLE topics ADD COLUMN ${columnName} ${definition}`)
  }
}

function ensureMetaCounterAtLeast(target) {
  const row = db.prepare("SELECT value FROM meta WHERE key = 'change_seq'").get()
  const current = Number(row?.value || 0)
  if (current >= target) return current
  db.prepare("INSERT INTO meta (key, value) VALUES ('change_seq', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value").run(target)
  return target
}

function backfillTopicSequenceIfNeeded() {
  const maxSeqRow = db.prepare('SELECT MAX(seq) AS max_seq FROM topics').get()
  const maxSeq = Number(maxSeqRow?.max_seq || 0)

  db.exec('UPDATE topics SET client_updated_at = updated_at WHERE client_updated_at IS NULL OR client_updated_at <= 0')

  if (maxSeq > 0) {
    ensureMetaCounterAtLeast(maxSeq)
    return
  }

  const topicIds = db
    .prepare('SELECT topic_id FROM topics ORDER BY updated_at ASC, topic_id ASC')
    .all()

  if (topicIds.length === 0) {
    ensureMetaCounterAtLeast(0)
    return
  }

  const updateSeq = db.prepare(`
    UPDATE topics
    SET seq = @seq,
        revision = CASE WHEN revision < 1 THEN 1 ELSE revision END
    WHERE topic_id = @topic_id
  `)

  let seq = 0
  db.transaction(() => {
    for (const row of topicIds) {
      seq += 1
      updateSeq.run({ topic_id: row.topic_id, seq })
    }
  })()

  ensureMetaCounterAtLeast(seq)
}

db.exec(`
  CREATE TABLE IF NOT EXISTS topics (
    topic_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    assistant_id TEXT,
    assistant_name TEXT,
    data        TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    deleted_at  INTEGER,
    seq         INTEGER NOT NULL DEFAULT 0,
    revision    INTEGER NOT NULL DEFAULT 0,
    client_updated_at INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT
  );

  CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
  );

  CREATE INDEX IF NOT EXISTS idx_topics_updated ON topics(updated_at);
  CREATE INDEX IF NOT EXISTS idx_topics_deleted ON topics(deleted_at);
  CREATE INDEX IF NOT EXISTS idx_topics_seq ON topics(seq);
`)

ensureTopicColumn('seq', 'INTEGER NOT NULL DEFAULT 0')
ensureTopicColumn('revision', 'INTEGER NOT NULL DEFAULT 0')
ensureTopicColumn('client_updated_at', 'INTEGER NOT NULL DEFAULT 0')
ensureTopicColumn('content_hash', 'TEXT')

ensureMetaCounterAtLeast(0)
backfillTopicSequenceIfNeeded()

// ── Prepared Statements ───────────────────────────────────────────────
const stmts = {
  getMeta: db.prepare('SELECT value FROM meta WHERE key = ?'),
  upsertMeta: db.prepare('INSERT INTO meta (key, value) VALUES (@key, @value) ON CONFLICT(key) DO UPDATE SET value = excluded.value'),

  getTopicForWrite: db.prepare(`
    SELECT topic_id, created_at, updated_at, deleted_at, seq, revision, client_updated_at, content_hash
    FROM topics
    WHERE topic_id = ?
  `),

  insertTopic: db.prepare(`
    INSERT INTO topics (
      topic_id,
      name,
      assistant_id,
      assistant_name,
      data,
      created_at,
      updated_at,
      deleted_at,
      seq,
      revision,
      client_updated_at,
      content_hash
    )
    VALUES (
      @topic_id,
      @name,
      @assistant_id,
      @assistant_name,
      @data,
      @created_at,
      @updated_at,
      NULL,
      @seq,
      @revision,
      @client_updated_at,
      @content_hash
    )
  `),

  updateTopic: db.prepare(`
    UPDATE topics
    SET name = @name,
        assistant_id = @assistant_id,
        assistant_name = @assistant_name,
        data = @data,
        updated_at = @updated_at,
        deleted_at = NULL,
        seq = @seq,
        revision = @revision,
        client_updated_at = @client_updated_at,
        content_hash = @content_hash
    WHERE topic_id = @topic_id
  `),

  softDelete: db.prepare(`
    UPDATE topics
    SET deleted_at = @now,
        updated_at = @now,
        seq = @seq,
        revision = @revision,
        client_updated_at = @client_updated_at
    WHERE topic_id = @topic_id
  `),

  getOne: db.prepare(`
    SELECT topic_id, name, assistant_id, assistant_name, data, created_at, updated_at, seq, revision, client_updated_at
    FROM topics
    WHERE topic_id = ? AND deleted_at IS NULL
  `),

  listAll: db.prepare(`
    SELECT topic_id, name, assistant_id, assistant_name, created_at, updated_at, seq, revision, client_updated_at
    FROM topics
    WHERE deleted_at IS NULL
    ORDER BY updated_at DESC
  `),

  changesAfterCursor: db.prepare(`
    SELECT topic_id, seq, revision, updated_at, client_updated_at, deleted_at, data
    FROM topics
    WHERE seq > @cursor
    ORDER BY seq ASC
    LIMIT @limit
  `),

  manifestAll: db.prepare('SELECT topic_id, revision, deleted_at FROM topics'),
}

function allocateSeq() {
  const row = stmts.getMeta.get('change_seq')
  const next = Number(row?.value || 0) + 1
  stmts.upsertMeta.run({ key: 'change_seq', value: next })
  return next
}

function getRequestWriteOptions(req) {
  const requestMeta = isRecord(req?.body?.syncMeta) ? req.body.syncMeta : null
  const headerForce = parseBoolean(headerFirst(req?.headers?.['x-sync-force']), false)
  const headerExpectedRevision = parseOptionalPositiveInt(headerFirst(req?.headers?.['x-sync-if-revision']))
  const bodyForce = requestMeta?.force === true || req?.body?.force === true
  const bodyExpectedRevision =
    parseOptionalPositiveInt(requestMeta?.expectedRevision) ??
    parseOptionalPositiveInt(req?.body?.expectedRevision)

  return {
    force: bodyForce || headerForce,
    expectedRevision: bodyExpectedRevision ?? headerExpectedRevision
  }
}

function getWriteOptionsFromPayload(payload, fallbackOptions = {}) {
  const payloadMeta = isRecord(payload?.syncMeta) ? payload.syncMeta : null
  const payloadForce = payloadMeta?.force === true || payload?.force === true
  const payloadExpectedRevision =
    parseOptionalPositiveInt(payloadMeta?.expectedRevision) ??
    parseOptionalPositiveInt(payload?.expectedRevision)

  return {
    force: payloadForce || fallbackOptions.force === true,
    expectedRevision: payloadExpectedRevision ?? fallbackOptions.expectedRevision ?? null
  }
}

function sanitizeTopicPayload(body) {
  if (!isRecord(body)) return body
  const topic = { ...body }
  delete topic.force
  delete topic.expectedRevision
  delete topic.syncMeta
  return topic
}

function buildConflictResult(topicId, existing, reason = 'revision_mismatch') {
  return {
    ok: true,
    topicId,
    status: 'conflict',
    error: reason,
    seq: Number(existing.seq || 0),
    revision: Number(existing.revision || 0),
    serverUpdatedAt: Number(existing.updated_at || 0),
    serverClientUpdatedAt: Number(existing.client_updated_at || 0),
    serverDeletedAt: existing.deleted_at == null ? null : Number(existing.deleted_at || 0)
  }
}

function applyTopicUpsert(body, requestOptions = {}) {
  const writeOptions = getWriteOptionsFromPayload(body, requestOptions)
  const topic = sanitizeTopicPayload(body)

  if (!topic || !topic.topicId) {
    return { ok: false, topicId: '', status: 'error', error: 'topicId is required' }
  }

  const topicId = String(topic.topicId)
  const now = Date.now()
  const createdAt = parseClientTimestamp(topic.createdAt) ?? now
  const clientUpdatedAt = parseClientTimestamp(topic.updatedAt) ?? now
  const payload = JSON.stringify(topic)
  const contentHash = sha1Hex(payload)

  const existing = stmts.getTopicForWrite.get(topicId)
  if (existing) {
    const existingRevision = Number(existing.revision || 0)
    const existingClientUpdatedAt = Number(existing.client_updated_at || 0)
    const existingDeleted = existing.deleted_at != null

    if (
      writeOptions.expectedRevision != null &&
      writeOptions.expectedRevision !== existingRevision &&
      !writeOptions.force
    ) {
      return buildConflictResult(topicId, existing, 'revision_mismatch')
    }

    if (!writeOptions.force) {
      // 统一 stale 判断：无论 deleted 与否，均用 < （相等时允许覆写/复活）
      if (clientUpdatedAt < existingClientUpdatedAt) {
        return {
          ok: true,
          topicId,
          status: 'stale',
          seq: Number(existing.seq || 0),
          revision: existingRevision,
        }
      }
    }

    if (
      !existingDeleted &&
      clientUpdatedAt === existingClientUpdatedAt &&
      contentHash === (existing.content_hash || '')
    ) {
      return {
        ok: true,
        topicId,
        status: 'noop',
        seq: Number(existing.seq || 0),
        revision: existingRevision,
      }
    }

    const seq = allocateSeq()
    const revision = existingRevision + 1

    stmts.updateTopic.run({
      topic_id: topicId,
      name: topic.name || '',
      assistant_id: topic.assistantId || null,
      assistant_name: topic.assistantName || null,
      data: payload,
      updated_at: now,
      seq,
      revision,
      client_updated_at: clientUpdatedAt,
      content_hash: contentHash,
    })

    return {
      ok: true,
      topicId,
      status: 'applied',
      seq,
      revision,
    }
  }

  if (writeOptions.expectedRevision != null && writeOptions.expectedRevision !== 0 && !writeOptions.force) {
    return {
      ok: true,
      topicId,
      status: 'conflict',
      error: 'topic_not_found_for_expected_revision',
      seq: 0,
      revision: 0
    }
  }

  const seq = allocateSeq()
  const revision = 1

  stmts.insertTopic.run({
    topic_id: topicId,
    name: topic.name || '',
    assistant_id: topic.assistantId || null,
    assistant_name: topic.assistantName || null,
    data: payload,
    created_at: createdAt,
    updated_at: now,
    seq,
    revision,
    client_updated_at: clientUpdatedAt,
    content_hash: contentHash,
  })

  return {
    ok: true,
    topicId,
    status: 'applied',
    seq,
    revision,
  }
}

function applyTopicDelete(topicId, requestOptions = {}) {
  if (!topicId) {
    return { ok: false, topicId: '', status: 'error', error: 'topicId is required' }
  }

  const normalizedTopicId = String(topicId)
  const existing = stmts.getTopicForWrite.get(topicId)
  if (!existing) {
    if (requestOptions.expectedRevision != null && !requestOptions.force) {
      return {
        ok: true,
        topicId: normalizedTopicId,
        status: 'conflict',
        error: 'topic_not_found_for_expected_revision',
        seq: 0,
        revision: 0
      }
    }
    return { ok: true, topicId: normalizedTopicId, status: 'not_found' }
  }

  const existingRevision = Number(existing.revision || 0)
  if (
    requestOptions.expectedRevision != null &&
    requestOptions.expectedRevision !== existingRevision &&
    !requestOptions.force
  ) {
    return buildConflictResult(normalizedTopicId, existing, 'revision_mismatch')
  }

  if (existing.deleted_at != null) {
    return {
      ok: true,
      topicId: normalizedTopicId,
      status: 'noop',
      seq: Number(existing.seq || 0),
      revision: existingRevision,
    }
  }

  const now = Date.now()
  const seq = allocateSeq()
  const revision = existingRevision + 1
  const clientUpdatedAt = parseClientTimestamp(requestOptions.clientUpdatedAt) ?? now

  stmts.softDelete.run({
    topic_id: normalizedTopicId,
    now,
    seq,
    revision,
    client_updated_at: clientUpdatedAt,
  })

  return {
    ok: true,
    topicId: normalizedTopicId,
    status: 'applied',
    seq,
    revision,
  }
}

function summarizeResults(results) {
  let applied = 0
  let noop = 0
  let stale = 0
  let conflict = 0
  let failed = 0

  for (const item of results) {
    if (!item.ok || item.status === 'error') {
      failed += 1
      continue
    }
    if (item.status === 'applied') {
      applied += 1
      continue
    }
    if (item.status === 'noop' || item.status === 'not_found') {
      noop += 1
      continue
    }
    if (item.status === 'stale') {
      stale += 1
      continue
    }

    if (item.status === 'conflict') {
      conflict += 1
      continue
    }
  }

  return { applied, noop, stale, conflict, failed }
}

const batchUpsert = db.transaction((topics, requestOptions = {}) => {
  const results = []
  for (const body of topics) {
    results.push(applyTopicUpsert(body, requestOptions))
  }
  return results
})

const batchDelete = db.transaction((topicIds, requestOptions = {}) => {
  const results = []
  for (const topicId of topicIds) {
    results.push(applyTopicDelete(topicId, requestOptions))
  }
  return results
})

// ── Express ───────────────────────────────────────────────────────────
const app = express()
app.use(
  cors(
    CORS_ORIGINS
      ? {
          origin: CORS_ORIGINS.split(',').map((s) => s.trim()).filter(Boolean),
          credentials: true
        }
      : undefined
  )
)
app.use(express.json({ limit: process.env.SYNC_BODY_LIMIT || '500mb' }))

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
    res.json({
      topicId: row.topic_id,
      name: row.name,
      assistantId: row.assistant_id,
      assistantName: row.assistant_name,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      seq: row.seq,
      revision: row.revision,
      clientUpdatedAt: row.client_updated_at,
      topic: JSON.parse(row.data),
      data: JSON.parse(row.data), // deprecated: use 'topic' instead
    })
  } catch (e) {
    console.error('[GET /api/topics/:id]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── GET /api/topics/:id/revision ── 获取 Topic 当前版本元数据 ───────────
app.get('/api/topics/:id/revision', (req, res) => {
  try {
    const row = stmts.getTopicForWrite.get(req.params.id)
    if (!row) {
      return res.status(404).json({ error: 'Topic not found' })
    }

    res.json({
      topicId: req.params.id,
      seq: Number(row.seq || 0),
      revision: Number(row.revision || 0),
      updatedAt: Number(row.updated_at || 0),
      clientUpdatedAt: Number(row.client_updated_at || 0),
      deletedAt: row.deleted_at == null ? null : Number(row.deleted_at || 0)
    })
  } catch (e) {
    console.error('[GET /api/topics/:id/revision]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── POST /api/topics ── 上传/更新 Topic ─────────────────────────────
app.post('/api/topics', (req, res) => {
  try {
    const requestOptions = getRequestWriteOptions(req)
    const result = applyTopicUpsert(req.body, requestOptions)
    if (!result.ok) {
      return res.status(400).json(result)
    }

    console.log(`[SYNC] Topic ${result.topicId} -> ${result.status}`)
    res.json(result)
  } catch (e) {
    console.error('[POST /api/topics]', e)
    res.status(500).json({ ok: false, status: 'error', error: e.message })
  }
})

// ── POST /api/topics/batch ── 批量上传 Topics（逐条回执）─────────────
app.post('/api/topics/batch', (req, res) => {
  try {
    const topics = req.body?.topics
    if (!Array.isArray(topics)) {
      return res.status(400).json({ ok: false, error: 'topics array is required' })
    }

    const requestOptions = getRequestWriteOptions(req)
    const results = batchUpsert(topics, requestOptions)
    const summary = summarizeResults(results)
    console.log(
      `[SYNC] Batch upsert -> applied=${summary.applied}, noop=${summary.noop}, ` +
        `stale=${summary.stale}, conflict=${summary.conflict}, failed=${summary.failed}`
    )

    res.json({
      ok: summary.failed === 0,
      ...summary,
      total: results.length,
      results,
    })
  } catch (e) {
    console.error('[POST /api/topics/batch]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── DELETE /api/topics/:id ── 软删除 Topic（含 seq/revision）─────────
app.delete('/api/topics/:id', (req, res) => {
  try {
    const requestOptions = getRequestWriteOptions(req)
    const result = applyTopicDelete(req.params.id, requestOptions)
    if (!result.ok) {
      return res.status(400).json(result)
    }

    console.log(`[SYNC] Delete ${result.topicId} -> ${result.status}`)
    res.json(result)
  } catch (e) {
    console.error('[DELETE /api/topics/:id]', e)
    res.status(500).json({ ok: false, status: 'error', error: e.message })
  }
})

// ── POST /api/topics/delete-batch ── 批量删除 Topic（逐条回执）────────
app.post('/api/topics/delete-batch', (req, res) => {
  try {
    const topicIds = req.body?.topicIds
    if (!Array.isArray(topicIds)) {
      return res.status(400).json({ ok: false, error: 'topicIds array is required' })
    }

    const ids = topicIds.map((item) => String(item || '').trim()).filter(Boolean)
    const requestOptions = getRequestWriteOptions(req)
    const results = batchDelete(ids, requestOptions)
    const summary = summarizeResults(results)
    console.log(
      `[SYNC] Batch delete -> applied=${summary.applied}, noop=${summary.noop}, ` +
        `conflict=${summary.conflict}, failed=${summary.failed}`
    )

    res.json({
      ok: summary.failed === 0,
      ...summary,
      total: results.length,
      results,
    })
  } catch (e) {
    console.error('[POST /api/topics/delete-batch]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── GET /api/sync/manifest ── 返回全量 topic revision 清单 ──────────
app.get('/api/sync/manifest', (req, res) => {
  try {
    const rows = stmts.manifestAll.all()
    const entries = {}
    for (const row of rows) {
      entries[row.topic_id] = {
        revision: Number(row.revision || 0),
        deletedAt: row.deleted_at == null ? null : Number(row.deleted_at)
      }
    }
    const changeSeq = Number(stmts.getMeta.get('change_seq')?.value || 0)
    res.json({ changeSeq, topicCount: rows.length, entries })
  } catch (e) {
    console.error('[GET /api/sync/manifest]', e)
    res.status(500).json({ error: e.message })
  }
})

// ── POST /api/topics/batch-get ── 批量拉取完整 topic 数据 ─────────────
app.post('/api/topics/batch-get', (req, res) => {
  try {
    const topicIds = req.body?.topicIds
    if (!Array.isArray(topicIds)) {
      return res.status(400).json({ ok: false, error: 'topicIds array is required' })
    }

    const ids = topicIds.map((item) => String(item || '').trim()).filter(Boolean)
    const topics = []
    for (const id of ids) {
      const row = stmts.getOne.get(id)
      if (!row) continue
      topics.push({
        topicId: row.topic_id,
        name: row.name,
        assistantId: row.assistant_id,
        assistantName: row.assistant_name,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        seq: row.seq,
        revision: row.revision,
        clientUpdatedAt: row.client_updated_at,
        topic: JSON.parse(row.data),
      })
    }

    res.json({ topics })
  } catch (e) {
    console.error('[POST /api/topics/batch-get]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── GET /api/sync/changes?cursor=<seq>&limit=<n>&includePayload=1 ──
app.get('/api/sync/changes', (req, res) => {
  try {
    const cursor = parsePositiveInt(req.query.cursor, 0)
    const limit = clamp(parsePositiveInt(req.query.limit, DEFAULT_PAGE_LIMIT), 1, MAX_PAGE_LIMIT)
    const includePayload = req.query.includePayload !== '0'

    const rows = stmts.changesAfterCursor.all({
      cursor,
      limit: limit + 1,
    })

    const hasMore = rows.length > limit
    const pageRows = hasMore ? rows.slice(0, limit) : rows

    const items = pageRows.map((row) => {
      const op = row.deleted_at != null ? 'delete' : 'upsert'
      const entry = {
        seq: Number(row.seq || 0),
        topicId: row.topic_id,
        op,
        revision: Number(row.revision || 0),
        updatedAt: Number(row.updated_at || 0),
        clientUpdatedAt: Number(row.client_updated_at || 0),
      }

      if (includePayload && op === 'upsert') {
        return {
          ...entry,
          topic: JSON.parse(row.data),
        }
      }

      return entry
    })

    const nextCursor = items.length > 0 ? items[items.length - 1].seq : cursor

    // 返回服务端最小可用 seq，客户端可据此检测 cursor 是否落入已 purge 的空洞
    const oldestRow = db.prepare('SELECT MIN(seq) AS min_seq FROM topics WHERE seq > 0').get()
    const oldestAvailableSeq = oldestRow?.min_seq != null ? Number(oldestRow.min_seq) : 0

    res.json({
      serverTime: Date.now(),
      cursor,
      nextCursor,
      hasMore,
      totalChanges: items.length,
      oldestAvailableSeq,
      items,
    })
  } catch (e) {
    console.error('[GET /api/sync/changes]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── GET /api/sync?since=<cursor> ── 兼容旧接口（仅返回 ID）─────────────
app.get('/api/sync', (req, res) => {
  try {
    const since = parsePositiveInt(req.query.since, 0)
    const rows = stmts.changesAfterCursor.all({ cursor: since, limit: MAX_PAGE_LIMIT + 1 })

    const upserts = []
    const deletes = []
    let lastSeq = since

    for (const row of rows) {
      lastSeq = Number(row.seq || lastSeq)
      if (row.deleted_at != null) deletes.push(row.topic_id)
      else upserts.push(row.topic_id)
    }

    res.json({
      serverTime: Date.now(),
      since,
      nextCursor: lastSeq,
      upserts,
      deletes,
      totalChanges: rows.length,
    })
  } catch (e) {
    console.error('[GET /api/sync]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── 软删除清理 ─────────────────────────────────────────────────────
const PURGE_RETENTION_MS = 30 * 24 * 60 * 60 * 1000 // 30 天
const PURGE_INTERVAL_MS = 24 * 60 * 60 * 1000 // 每 24 小时执行一次

const stmtPurgeDeleted = db.prepare(`
  DELETE FROM topics
  WHERE deleted_at IS NOT NULL AND deleted_at < ?
`)

function purgeDeletedTopics() {
  const cutoff = Date.now() - PURGE_RETENTION_MS
  const result = stmtPurgeDeleted.run(cutoff)
  if (result.changes > 0) {
    console.log(`[PURGE] Removed ${result.changes} soft-deleted topics older than 30 days`)
  }
  return result.changes
}

// 启动时执行一次，之后每 24 小时执行
purgeDeletedTopics()
setInterval(purgeDeletedTopics, PURGE_INTERVAL_MS)

// ── POST /api/admin/purge ── 手动触发清理（需认证）────────────────────
app.post('/api/admin/purge', (req, res) => {
  try {
    const retentionDays = parsePositiveInt(req.query.retentionDays, 30)
    const cutoff = Date.now() - retentionDays * 24 * 60 * 60 * 1000
    const result = stmtPurgeDeleted.run(cutoff)
    console.log(`[PURGE] Manual purge: removed ${result.changes} topics (retention=${retentionDays}d)`)
    res.json({ ok: true, purged: result.changes, retentionDays })
  } catch (e) {
    console.error('[POST /api/admin/purge]', e)
    res.status(500).json({ ok: false, error: e.message })
  }
})

// ── 健康检查 ────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const count = db.prepare('SELECT COUNT(*) as count FROM topics WHERE deleted_at IS NULL').get()
  const deletedCount = db.prepare('SELECT COUNT(*) as count FROM topics WHERE deleted_at IS NOT NULL').get()
  const changeSeq = Number(stmts.getMeta.get('change_seq')?.value || 0)
  res.json({ status: 'ok', topics: count.count, deletedTopics: deletedCount.count, changeSeq, uptime: process.uptime() })
})

// ── 启动 ────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  const count = db.prepare('SELECT COUNT(*) as count FROM topics WHERE deleted_at IS NULL').get()
  const changeSeq = Number(stmts.getMeta.get('change_seq')?.value || 0)
  console.log(`🍒 Cherry Sync Server listening on http://localhost:${PORT}`)
  console.log(`   Database: ${DB_PATH}`)
  console.log(`   Topics: ${count.count}`)
  console.log(`   ChangeSeq: ${changeSeq}`)
  console.log('   Token configured: yes')
})

process.on('SIGINT', () => {
  db.close()
  process.exit(0)
})
