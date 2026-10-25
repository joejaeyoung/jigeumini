CREATE TABLE processed_event (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    event_id uuid NOT NULL,
    channel_id varchar(32) NOT NULL,
    collection_session_id uuid NOT NULL,
    event_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    source_topic varchar(249) NOT NULL,
    source_partition int NOT NULL,
    source_offset bigint NOT NULL,
    payload_hash varchar(64) NOT NULL,
    PRIMARY KEY (projection, run_id, event_id)
);
CREATE TABLE chat_bucket_10s (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    channel_id varchar(32) NOT NULL,
    bucket_start timestamptz NOT NULL,
    chat_count bigint NOT NULL CHECK (chat_count > 0),
    PRIMARY KEY (projection, run_id, channel_id, bucket_start)
);
CREATE INDEX chat_bucket_time ON chat_bucket_10s (projection, run_id, bucket_start, channel_id);
CREATE TABLE projection_progress (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    partition int NOT NULL,
    next_resolved_offset bigint NOT NULL,
    checked_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, run_id, topic, partition)
);
