CREATE TABLE portability_outbox (
    id VARCHAR(36) PRIMARY KEY,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(128) NOT NULL,
    payload VARCHAR(4000) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    published_at TIMESTAMP NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error VARCHAR(2000) NULL
);

CREATE INDEX idx_outbox_published_created ON portability_outbox(published_at, created_at);

CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);
