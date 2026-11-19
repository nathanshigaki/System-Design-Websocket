CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    seq_id BIGINT UNIQUE NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    content TEXT NOT NULL,
    gateway_id VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_messages_seq_id ON messages (seq_id);

CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notifications_id ON notifications (id);
