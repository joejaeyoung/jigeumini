CREATE TABLE spike_outbox (
    spike_id uuid PRIMARY KEY REFERENCES spike_event(spike_id),
    run_id varchar(80) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz
);
CREATE INDEX spike_outbox_pending ON spike_outbox(run_id,created_at,spike_id) WHERE published_at IS NULL;
CREATE TABLE notification_event (
    run_id varchar(80) NOT NULL,
    spike_id uuid NOT NULL,
    channel_id varchar(32) NOT NULL,
    evaluated_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY(run_id,spike_id)
);
CREATE INDEX notification_feed ON notification_event(run_id,channel_id,evaluated_at DESC,spike_id DESC);
CREATE TABLE notification_progress (
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    partition int NOT NULL,
    next_offset bigint NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY(run_id,topic,partition)
);
