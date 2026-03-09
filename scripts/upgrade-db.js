#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const Database = require('better-sqlite3')

function sha1Hex(value) {
  return crypto.createHash('sha1').update(value).digest('hex')
}

function hasFlag(argv, flag) {
  return argv.includes(flag)
}

function getArgValue(argv, flag) {
  const index = argv.indexOf(flag)
  if (index === -1) return undefined
  const value = argv[index + 1]
  if (!value || value.startsWith('--')) return undefined
  return value
}

function ensureTopicColumn(db, columnName, definition) {
  const columns = db.prepare('PRAGMA table_info(topics)').all().map((item) => item.name)
  if (!columns.includes(columnName)) {
    db.exec(`ALTER TABLE topics ADD COLUMN ${columnName} ${definition}`)
  }
}

function ensureSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS topics (
      topic_id TEXT PRIMARY KEY,
      name TEXT NOT NULL DEFAULT '',
      assistant_id TEXT,
      assistant_name TEXT,
      data TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      deleted_at INTEGER
    );

    CREATE TABLE IF NOT EXISTS meta (
      key TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    );
  `)

  ensureTopicColumn(db, 'seq', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'revision', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'client_updated_at', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'content_hash', 'TEXT')

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_topics_updated ON topics(updated_at);
    CREATE INDEX IF NOT EXISTS idx_topics_deleted ON topics(deleted_at);
    CREATE INDEX IF NOT EXISTS idx_topics_seq ON topics(seq);
  `)
}

function parseTimestamp(value) {
  if (value == null) return null
  if (typeof value === 'number' && Number.isFinite(value) && value > 0) {
    return Math.floor(value)
  }

  const parsed = new Date(value).getTime()
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null
}

function toIsoString(timestamp, fallbackTimestamp) {
  const effective = Number.isFinite(timestamp) && timestamp > 0 ? timestamp : fallbackTimestamp
  return new Date(effective).toISOString()
}

function normalizeBlocks(rawBlocks, messageId, baseTimestamp) {
  if (!Array.isArray(rawBlocks)) return []

  return rawBlocks
    .filter((block) => block && typeof block === 'object' && !Array.isArray(block))
    .map((block, index) => {
      const blockId =
        typeof block.id === 'string' && block.id.trim()
          ? block.id.trim()
          : sha1Hex(`${messageId}#blk:${index}`)

      return {
        ...block,
        id: blockId,
        messageId,
        createdAt: typeof block.createdAt === 'string' ? block.createdAt : toIsoString(baseTimestamp, baseTimestamp),
        status: typeof block.status === 'string' && block.status.trim() ? block.status : 'success'
      }
    })
}

function normalizeMessages(messages, topicId, topicUpdatedAt) {
  if (!Array.isArray(messages)) {
    return { messages: [], repairedAskIds: 0, generatedMessageIds: 0 }
  }

  const seenUserIds = new Set()
  let lastUserId
  let repairedAskIds = 0
  let generatedMessageIds = 0

  const normalizedMessages = messages
    .filter((message) => message && typeof message === 'object' && !Array.isArray(message))
    .map((message, index) => {
      const role =
        message.role === 'user' || message.role === 'assistant' || message.role === 'system'
          ? message.role
          : 'assistant'

      const existingId = typeof message.id === 'string' && message.id.trim() ? message.id.trim() : undefined
      const textSeed = JSON.stringify(message.blocks || message.content || '')
      const messageId = existingId || sha1Hex(`${topicId}#msg:${index}:${role}:${textSeed}`)
      if (!existingId) generatedMessageIds += 1

      const baseTimestamp = parseTimestamp(message.createdAt) || topicUpdatedAt + index
      const rawAskId = typeof message.askId === 'string' && message.askId.trim() ? message.askId.trim() : undefined
      const normalizedAskId =
        role === 'assistant'
          ? rawAskId && seenUserIds.has(rawAskId)
            ? rawAskId
            : lastUserId
          : undefined

      if (role === 'assistant') {
        if (normalizedAskId !== rawAskId) {
          repairedAskIds += 1
        }
      } else if (rawAskId) {
        repairedAskIds += 1
      }

      const normalized = {
        ...message,
        id: messageId,
        role,
        createdAt: typeof message.createdAt === 'string' ? message.createdAt : toIsoString(baseTimestamp, topicUpdatedAt),
        status: typeof message.status === 'string' && message.status.trim() ? message.status : 'success',
        useful: typeof message.useful === 'boolean' ? message.useful : true,
        blocks: normalizeBlocks(message.blocks, messageId, baseTimestamp)
      }

      if (role === 'assistant' && normalizedAskId) {
        normalized.askId = normalizedAskId
      } else {
        delete normalized.askId
      }

      if (role === 'user') {
        seenUserIds.add(messageId)
        lastUserId = messageId
      }

      return normalized
    })

  return { messages: normalizedMessages, repairedAskIds, generatedMessageIds }
}

function normalizeTopicRow(row) {
  let parsed
  try {
    parsed = JSON.parse(row.data)
  } catch (error) {
    parsed = {}
  }

  const fallbackCreatedAt = parseTimestamp(row.created_at) || Date.now()
  const fallbackUpdatedAt = parseTimestamp(row.updated_at) || fallbackCreatedAt
  const createdAt = parseTimestamp(parsed.createdAt) || fallbackCreatedAt
  const updatedAt = Math.max(parseTimestamp(parsed.updatedAt) || fallbackUpdatedAt, createdAt)

  const assistantId =
    typeof parsed.assistantId === 'string' && parsed.assistantId.trim()
      ? parsed.assistantId.trim()
      : typeof row.assistant_id === 'string' && row.assistant_id.trim()
        ? row.assistant_id.trim()
        : null

  const assistantName =
    typeof parsed.assistantName === 'string' && parsed.assistantName.trim()
      ? parsed.assistantName.trim()
      : typeof row.assistant_name === 'string'
        ? row.assistant_name
        : ''

  const name =
    typeof parsed.name === 'string' && parsed.name.trim()
      ? parsed.name.trim()
      : typeof row.name === 'string' && row.name.trim()
        ? row.name.trim()
        : 'Untitled'

  const normalizedMessages = normalizeMessages(parsed.messages, row.topic_id, updatedAt)
  const normalizedTopic = {
    ...parsed,
    topicId: row.topic_id,
    name,
    assistantId,
    assistantName,
    createdAt: toIsoString(createdAt, fallbackCreatedAt),
    updatedAt: toIsoString(updatedAt, fallbackUpdatedAt),
    messages: normalizedMessages.messages
  }

  const data = JSON.stringify(normalizedTopic)
  return {
    topicId: row.topic_id,
    name,
    assistantId,
    assistantName,
    deletedAt: parseTimestamp(row.deleted_at),
    createdAt,
    updatedAt,
    clientUpdatedAt: updatedAt,
    data,
    contentHash: sha1Hex(data),
    repairedAskIds: normalizedMessages.repairedAskIds,
    generatedMessageIds: normalizedMessages.generatedMessageIds
  }
}

function backupDatabase(dbPath) {
  const dir = path.dirname(dbPath)
  const ext = path.extname(dbPath)
  const base = path.basename(dbPath, ext)
  const stamp = new Date().toISOString().replace(/[:.]/g, '-')
  const backupPath = path.join(dir, `${base}.bak.${stamp}${ext}`)
  fs.copyFileSync(dbPath, backupPath)
  return backupPath
}

function main() {
  const argv = process.argv.slice(2)
  const dbPath = getArgValue(argv, '--db') || path.join(__dirname, '..', 'data', 'sync.db')
  const apply = hasFlag(argv, '--apply')
  const skipBackup = hasFlag(argv, '--skip-backup')

  if (!fs.existsSync(dbPath)) {
    console.error(`Database not found: ${dbPath}`)
    process.exit(1)
  }

  const db = new Database(dbPath)
  db.pragma('journal_mode = WAL')
  ensureSchema(db)

  const rows = db
    .prepare(`
      SELECT topic_id, name, assistant_id, assistant_name, data, created_at, updated_at, deleted_at
      FROM topics
      ORDER BY COALESCE(updated_at, created_at, 0) ASC, topic_id ASC
    `)
    .all()

  const normalizedRows = rows.map(normalizeTopicRow)
  const orderedRows = [...normalizedRows].sort((a, b) => {
    if (a.updatedAt !== b.updatedAt) return a.updatedAt - b.updatedAt
    return String(a.topicId).localeCompare(String(b.topicId))
  })

  let repairedAskIds = 0
  let generatedMessageIds = 0
  for (const row of orderedRows) {
    repairedAskIds += row.repairedAskIds
    generatedMessageIds += row.generatedMessageIds
  }

  console.log(
    `[dry-run=${!apply}] topics=${orderedRows.length} repairedAskIds=${repairedAskIds} generatedMessageIds=${generatedMessageIds}`
  )

  if (!apply) {
    return
  }

  let backupPath = null
  if (!skipBackup) {
    backupPath = backupDatabase(dbPath)
  }

  const updateTopic = db.prepare(`
    UPDATE topics
    SET name = @name,
        assistant_id = @assistant_id,
        assistant_name = @assistant_name,
        data = @data,
        created_at = @created_at,
        updated_at = @updated_at,
        seq = @seq,
        revision = @revision,
        client_updated_at = @client_updated_at,
        content_hash = @content_hash
    WHERE topic_id = @topic_id
  `)
  const upsertMeta = db.prepare(`
    INSERT INTO meta (key, value)
    VALUES (@key, @value)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `)

  db.transaction(() => {
    orderedRows.forEach((row, index) => {
      const seq = index + 1
      updateTopic.run({
        topic_id: row.topicId,
        name: row.name,
        assistant_id: row.assistantId,
        assistant_name: row.assistantName,
        data: row.data,
        created_at: row.createdAt,
        updated_at: row.updatedAt,
        seq,
        revision: 1,
        client_updated_at: row.clientUpdatedAt,
        content_hash: row.contentHash
      })
    })

    upsertMeta.run({ key: 'change_seq', value: orderedRows.length })
  })()

  console.log(
    `[applied] topics=${orderedRows.length} change_seq=${orderedRows.length}${backupPath ? ` backup=${backupPath}` : ''}`
  )
}

main()
