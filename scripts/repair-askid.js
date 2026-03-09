#!/usr/bin/env node

const path = require('path')
const crypto = require('crypto')
const Database = require('better-sqlite3')

function sha1Hex(value) {
  return crypto.createHash('sha1').update(value).digest('hex')
}

function parseListArg(argv, flag) {
  const values = []
  for (let index = 0; index < argv.length; index += 1) {
    if (argv[index] !== flag) continue
    const raw = argv[index + 1]
    if (!raw || raw.startsWith('--')) continue
    values.push(
      ...String(raw)
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
    )
  }
  return values
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

function normalizeTopicPayload(topicData) {
  if (!topicData || typeof topicData !== 'object' || !Array.isArray(topicData.messages)) {
    return { changed: false, repairedMessages: 0, clearedUserAskIds: 0, topicData }
  }

  const seenUserIds = new Set()
  let lastUserId
  let repairedMessages = 0
  let clearedUserAskIds = 0

  const nextMessages = topicData.messages.map((message) => {
    if (!message || typeof message !== 'object' || Array.isArray(message)) {
      return message
    }

    const next = { ...message }
    const role = next.role
    const messageId = typeof next.id === 'string' && next.id ? next.id : undefined
    const rawAskId =
      typeof next.askId === 'string' && next.askId.trim() ? next.askId.trim() : undefined

    if (role === 'assistant') {
      const normalizedAskId = rawAskId && seenUserIds.has(rawAskId) ? rawAskId : lastUserId

      if (normalizedAskId) {
        if (next.askId !== normalizedAskId) {
          next.askId = normalizedAskId
          repairedMessages += 1
        }
      } else if ('askId' in next) {
        delete next.askId
        repairedMessages += 1
      }
    } else {
      if ('askId' in next && next.askId != null) {
        delete next.askId
        clearedUserAskIds += 1
      }

      if (role === 'user' && messageId) {
        seenUserIds.add(messageId)
        lastUserId = messageId
      }
    }

    return next
  })

  if (repairedMessages === 0 && clearedUserAskIds === 0) {
    return { changed: false, repairedMessages, clearedUserAskIds, topicData }
  }

  return {
    changed: true,
    repairedMessages,
    clearedUserAskIds,
    topicData: {
      ...topicData,
      messages: nextMessages
    }
  }
}

function ensureTopicColumn(db, columnName, definition) {
  const columns = db.prepare('PRAGMA table_info(topics)').all().map((item) => item.name)
  if (!columns.includes(columnName)) {
    db.exec(`ALTER TABLE topics ADD COLUMN ${columnName} ${definition}`)
  }
}

function printUsage() {
  console.log(`Usage:
  node scripts/repair-askid.js [--db /path/to/sync.db] [--assistant-name NAME] [--topic-id ID] [--apply]

Examples:
  node scripts/repair-askid.js --assistant-name Gemini --assistant-name ChatGPT
  node scripts/repair-askid.js --assistant-name "ChatGPT 导入" --assistant-name POE --apply
  node scripts/repair-askid.js --topic-id topic-1 --topic-id topic-2 --apply`)
}

function main() {
  const argv = process.argv.slice(2)
  if (hasFlag(argv, '--help') || hasFlag(argv, '-h')) {
    printUsage()
    process.exit(0)
  }

  const dbPath = getArgValue(argv, '--db') || path.join(__dirname, '..', 'data', 'sync.db')
  const assistantNames = new Set(parseListArg(argv, '--assistant-name'))
  const topicIds = new Set(parseListArg(argv, '--topic-id'))
  const apply = hasFlag(argv, '--apply')

  const db = new Database(dbPath)
  db.pragma('journal_mode = WAL')
  db.exec(`
    CREATE TABLE IF NOT EXISTS meta (
      key   TEXT PRIMARY KEY,
      value INTEGER NOT NULL
    )
  `)
  ensureTopicColumn(db, 'seq', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'revision', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'client_updated_at', 'INTEGER NOT NULL DEFAULT 0')
  ensureTopicColumn(db, 'content_hash', 'TEXT')

  const topics = db
    .prepare(
      `SELECT topic_id, assistant_name, data, revision, seq, updated_at, client_updated_at
       FROM topics
       WHERE deleted_at IS NULL
       ORDER BY updated_at ASC, topic_id ASC`
    )
    .all()

  const filtered = topics.filter((row) => {
    if (topicIds.size > 0 && !topicIds.has(row.topic_id)) {
      return false
    }
    if (assistantNames.size > 0 && !assistantNames.has(row.assistant_name || '')) {
      return false
    }
    return true
  })

  let scannedTopics = 0
  let changedTopics = 0
  let repairedMessages = 0
  let clearedUserAskIds = 0
  const updates = []

  for (const row of filtered) {
    scannedTopics += 1

    let topicData
    try {
      topicData = JSON.parse(row.data)
    } catch (error) {
      console.warn(`[skip] ${row.topic_id}: invalid JSON (${error.message})`)
      continue
    }

    const result = normalizeTopicPayload(topicData)
    if (!result.changed) {
      continue
    }

    changedTopics += 1
    repairedMessages += result.repairedMessages
    clearedUserAskIds += result.clearedUserAskIds
    updates.push({
      topicId: row.topic_id,
      assistantName: row.assistant_name || '',
      data: JSON.stringify(result.topicData),
      previousRevision: Number(row.revision || 0),
      previousUpdatedAt: Number(row.updated_at || 0)
    })
  }

  console.log(
    `[dry-run=${!apply}] scanned=${scannedTopics} changed=${changedTopics} repairedAssistantAskIds=${repairedMessages} clearedUserAskIds=${clearedUserAskIds}`
  )

  if (updates.length > 0) {
    for (const item of updates.slice(0, 20)) {
      console.log(`  - ${item.topicId} [${item.assistantName}]`)
    }
    if (updates.length > 20) {
      console.log(`  ... and ${updates.length - 20} more topics`)
    }
  }

  if (!apply || updates.length === 0) {
    return
  }

  const maxSeqRow = db.prepare('SELECT COALESCE(MAX(seq), 0) AS maxSeq FROM topics').get()
  let nextSeq = Number(maxSeqRow?.maxSeq || 0)
  const updateTopic = db.prepare(`
    UPDATE topics
    SET data = @data,
        updated_at = @updated_at,
        seq = @seq,
        revision = @revision,
        client_updated_at = @client_updated_at,
        content_hash = @content_hash
    WHERE topic_id = @topic_id
  `)
  const updateMeta = db.prepare(`
    INSERT INTO meta (key, value)
    VALUES ('change_seq', @value)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value
  `)

  const nowBase = Date.now()
  db.transaction(() => {
    updates.forEach((item, index) => {
      nextSeq += 1
      const updatedAt = Math.max(nowBase + index, item.previousUpdatedAt + 1)
      updateTopic.run({
        topic_id: item.topicId,
        data: item.data,
        updated_at: updatedAt,
        seq: nextSeq,
        revision: item.previousRevision + 1,
        client_updated_at: updatedAt,
        content_hash: sha1Hex(item.data)
      })
    })

    updateMeta.run({ value: nextSeq })
  })()

  console.log(`[applied] updatedTopics=${updates.length} finalChangeSeq=${nextSeq}`)
}

main()
