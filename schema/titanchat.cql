-- ═══════════════════════════════════════════════════════════════
--  TitanChat — Cassandra Schema
--  Run once: cqlsh -f shared/schema.cql
--
--  Production: Change SimpleStrategy → NetworkTopologyStrategy
--  with replication_factor 3 per data center.
-- ═══════════════════════════════════════════════════════════════

CREATE KEYSPACE IF NOT EXISTS titanchat
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
  AND durable_writes = true;

USE titanchat;

-- Users
CREATE TABLE IF NOT EXISTS users (
    user_id         TEXT PRIMARY KEY,
    display_name    TEXT,
    email           TEXT,
    avatar_url      TEXT,
    role            TEXT,
    status          TEXT,
    created_at      TIMESTAMP,
    last_seen_at    TIMESTAMP,
    metadata        MAP<TEXT, TEXT>
);

-- Conversations
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id   TEXT PRIMARY KEY,
    bot_id            TEXT,
    user_id           TEXT,
    agent_id          TEXT,
    server_id         TEXT,
    status            TEXT,
    channel           TEXT,
    subject           TEXT,
    started_at        TIMESTAMP,
    ended_at          TIMESTAMP,
    last_message_at   TIMESTAMP,
    last_message_preview TEXT,
    unread_user       INT,
    unread_agent      INT,
    metadata          MAP<TEXT, TEXT>
);

-- Inbox: conversations by user (sorted by last_message_at DESC)
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id             TEXT,
    last_message_at     TIMESTAMP,
    conversation_id     TEXT,
    agent_id            TEXT,
    status              TEXT,
    subject             TEXT,
    last_message_preview TEXT,
    unread_count        INT,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);

-- Agent dashboard
CREATE TABLE IF NOT EXISTS conversations_by_agent (
    agent_id            TEXT,
    status              TEXT,
    last_message_at     TIMESTAMP,
    conversation_id     TEXT,
    user_id             TEXT,
    subject             TEXT,
    last_message_preview TEXT,
    unread_count        INT,
    PRIMARY KEY ((agent_id, status), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);

-- Bot-scoped view: conversations by bot_id (for admin dashboard)
CREATE TABLE IF NOT EXISTS conversations_by_bot (
    bot_id              TEXT,
    started_at          TIMESTAMP,
    conversation_id     TEXT,
    user_id             TEXT,
    agent_id            TEXT,
    status              TEXT,
    subject             TEXT,
    last_message_preview TEXT,
    last_message_at     TIMESTAMP,
    PRIMARY KEY (bot_id, started_at, conversation_id)
) WITH CLUSTERING ORDER BY (started_at DESC, conversation_id ASC);

-- Admin view: all conversations by status + day bucket
-- Query: "show me all active chats from today"
CREATE TABLE IF NOT EXISTS active_conversations (
    status              TEXT,
    bucket              TEXT,
    started_at          TIMESTAMP,
    conversation_id     TEXT,
    user_id             TEXT,
    agent_id            TEXT,
    subject             TEXT,
    last_message_at     TIMESTAMP,
    last_message_preview TEXT,
    PRIMARY KEY ((status, bucket), started_at, conversation_id)
) WITH CLUSTERING ORDER BY (started_at DESC, conversation_id ASC);

-- Messages (partitioned by conversation + day bucket)
CREATE TABLE IF NOT EXISTS messages (
    conversation_id   TEXT,
    bucket            TEXT,
    created_at        TIMESTAMP,
    message_id        TEXT,
    sender_id         TEXT,
    sender_type       TEXT,
    content_type      TEXT,
    content           TEXT,
    reply_to_id       TEXT,
    seq               INT,
    status            TEXT,
    edited            BOOLEAN,
    deleted           BOOLEAN,
    PRIMARY KEY ((conversation_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at ASC, message_id ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': 1
  };

-- Ack receipts (bucketed to prevent unbounded partition growth)
CREATE TABLE IF NOT EXISTS message_acks (
    conversation_id   TEXT,
    bucket            TEXT,
    message_id        TEXT,
    ack_type          TEXT,
    acked_by          TEXT,
    acked_at          TIMESTAMP,
    PRIMARY KEY ((conversation_id, bucket), message_id, ack_type)
);

-- Attachments
CREATE TABLE IF NOT EXISTS attachments (
    conversation_id   TEXT,
    message_id        TEXT,
    attachment_id     TEXT,
    file_name         TEXT,
    file_type         TEXT,
    file_size         BIGINT,
    storage_url       TEXT,
    thumbnail_url     TEXT,
    uploaded_at       TIMESTAMP,
    PRIMARY KEY ((conversation_id, message_id), attachment_id)
);

-- Unread counters (counter table)
CREATE TABLE IF NOT EXISTS unread_counters (
    user_id             TEXT,
    conversation_id     TEXT,
    count               COUNTER,
    PRIMARY KEY (user_id, conversation_id)
);

-- Agent queue (sharded by priority to avoid single hot partition)
-- Note: Use TTL 604800 (7 days) on inserts to auto-expire stale entries
CREATE TABLE IF NOT EXISTS agent_queue (
    queue_id            TEXT,
    priority            INT,
    queued_at           TIMESTAMP,
    conversation_id     TEXT,
    user_id             TEXT,
    subject             TEXT,
    PRIMARY KEY ((queue_id, priority), queued_at, conversation_id)
) WITH CLUSTERING ORDER BY (queued_at ASC, conversation_id ASC);

-- Agent assignment history
CREATE TABLE IF NOT EXISTS agent_assignments (
    conversation_id   TEXT,
    assigned_at       TIMESTAMP,
    agent_id          TEXT,
    assigned_by       TEXT,
    ended_at          TIMESTAMP,
    PRIMARY KEY (conversation_id, assigned_at)
) WITH CLUSTERING ORDER BY (assigned_at DESC);
