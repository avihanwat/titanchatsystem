"""Rebuild titanchat schema - drops and recreates all tables with fresh metadata."""
import time
from shared.db.cassandra import get_session

s = get_session()

tables = [
    'active_conversations', 'agent_assignments', 'agent_queue', 'attachments',
    'conversations', 'conversations_by_agent', 'conversations_by_bot',
    'conversations_by_user', 'message_acks', 'messages', 'unread_counters', 'users'
]

print("Dropping tables...")
for t in tables:
    try:
        s.execute(f"DROP TABLE IF EXISTS titanchat.{t}")
        print(f"  Dropped {t}")
    except Exception as e:
        print(f"  Error dropping {t}: {e}")

print("\nWaiting for schema agreement...")
time.sleep(3)

s.execute("USE titanchat")

print("Creating tables...")

s.execute("""CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT,
    email TEXT,
    avatar_url TEXT,
    role TEXT,
    status TEXT,
    created_at TIMESTAMP,
    last_seen_at TIMESTAMP,
    metadata MAP<TEXT, TEXT>
)""")
print("  Created users")

s.execute("""CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    bot_id TEXT,
    user_id TEXT,
    agent_id TEXT,
    server_id TEXT,
    status TEXT,
    channel TEXT,
    subject TEXT,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    last_message_at TIMESTAMP,
    last_message_preview TEXT,
    unread_user INT,
    unread_agent INT,
    metadata MAP<TEXT, TEXT>
)""")
print("  Created conversations")

s.execute("""CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id TEXT,
    last_message_at TIMESTAMP,
    conversation_id TEXT,
    agent_id TEXT,
    status TEXT,
    subject TEXT,
    last_message_preview TEXT,
    unread_count INT,
    PRIMARY KEY (user_id, last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)""")
print("  Created conversations_by_user")

s.execute("""CREATE TABLE IF NOT EXISTS conversations_by_agent (
    agent_id TEXT,
    status TEXT,
    last_message_at TIMESTAMP,
    conversation_id TEXT,
    user_id TEXT,
    subject TEXT,
    last_message_preview TEXT,
    unread_count INT,
    PRIMARY KEY ((agent_id, status), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC)""")
print("  Created conversations_by_agent")

s.execute("""CREATE TABLE IF NOT EXISTS conversations_by_bot (
    bot_id TEXT,
    started_at TIMESTAMP,
    conversation_id TEXT,
    user_id TEXT,
    agent_id TEXT,
    status TEXT,
    subject TEXT,
    last_message_preview TEXT,
    last_message_at TIMESTAMP,
    PRIMARY KEY (bot_id, started_at, conversation_id)
) WITH CLUSTERING ORDER BY (started_at DESC, conversation_id ASC)""")
print("  Created conversations_by_bot")

s.execute("""CREATE TABLE IF NOT EXISTS active_conversations (
    status TEXT,
    bucket TEXT,
    started_at TIMESTAMP,
    conversation_id TEXT,
    user_id TEXT,
    agent_id TEXT,
    subject TEXT,
    last_message_at TIMESTAMP,
    last_message_preview TEXT,
    PRIMARY KEY ((status, bucket), started_at, conversation_id)
) WITH CLUSTERING ORDER BY (started_at DESC, conversation_id ASC)""")
print("  Created active_conversations")

s.execute("""CREATE TABLE IF NOT EXISTS messages (
    conversation_id TEXT,
    bucket TEXT,
    created_at TIMESTAMP,
    message_id TEXT,
    sender_id TEXT,
    sender_type TEXT,
    content_type TEXT,
    content TEXT,
    reply_to_id TEXT,
    seq INT,
    status TEXT,
    edited BOOLEAN,
    deleted BOOLEAN,
    PRIMARY KEY ((conversation_id, bucket), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at ASC, message_id ASC)
  AND compaction = {
    'class': 'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'DAYS',
    'compaction_window_size': '1'
  }""")
print("  Created messages")

s.execute("""CREATE TABLE IF NOT EXISTS message_acks (
    conversation_id TEXT,
    bucket TEXT,
    message_id TEXT,
    ack_type TEXT,
    acked_by TEXT,
    acked_at TIMESTAMP,
    PRIMARY KEY ((conversation_id, bucket), message_id, ack_type)
)""")
print("  Created message_acks")

s.execute("""CREATE TABLE IF NOT EXISTS attachments (
    conversation_id TEXT,
    message_id TEXT,
    attachment_id TEXT,
    file_name TEXT,
    file_type TEXT,
    file_size BIGINT,
    storage_url TEXT,
    thumbnail_url TEXT,
    uploaded_at TIMESTAMP,
    PRIMARY KEY ((conversation_id, message_id), attachment_id)
)""")
print("  Created attachments")

s.execute("""CREATE TABLE IF NOT EXISTS unread_counters (
    user_id TEXT,
    conversation_id TEXT,
    count COUNTER,
    PRIMARY KEY (user_id, conversation_id)
)""")
print("  Created unread_counters")

s.execute("""CREATE TABLE IF NOT EXISTS agent_queue (
    queue_id TEXT,
    priority INT,
    queued_at TIMESTAMP,
    conversation_id TEXT,
    user_id TEXT,
    subject TEXT,
    PRIMARY KEY ((queue_id, priority), queued_at, conversation_id)
) WITH CLUSTERING ORDER BY (queued_at ASC, conversation_id ASC)""")
print("  Created agent_queue")

s.execute("""CREATE TABLE IF NOT EXISTS agent_assignments (
    conversation_id TEXT,
    assigned_at TIMESTAMP,
    agent_id TEXT,
    assigned_by TEXT,
    ended_at TIMESTAMP,
    PRIMARY KEY (conversation_id, assigned_at)
) WITH CLUSTERING ORDER BY (assigned_at DESC)""")
print("  Created agent_assignments")

print("\nWaiting for schema agreement...")
time.sleep(3)

print("\nVerifying metadata...")
rows = list(s.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'titanchat'"))
print(f"Total tables in titanchat: {len(rows)}")
for r in sorted(rows, key=lambda x: x.table_name):
    print(f"  {r.table_name}")

# Verify columns metadata exists
cols = list(s.execute("SELECT table_name, column_name FROM system_schema.columns WHERE keyspace_name = 'titanchat'"))
print(f"\nTotal columns in metadata: {len(cols)}")

print("\nDONE - All tables recreated with fresh metadata.")
print("Now refresh your DBeaver connection (right-click → Refresh)")
