CREATE TABLE collection_channel (
    channel_id varchar(32) PRIMARY KEY,
    collector_id uuid NOT NULL,
    enabled boolean NOT NULL,
    observed_since timestamptz NOT NULL,
    state varchar(32) NOT NULL,
    updated_at timestamptz NOT NULL
);
CREATE TABLE collection_session (
    session_id uuid PRIMARY KEY,
    channel_id varchar(32) NOT NULL,
    collector_id uuid NOT NULL,
    connected_at timestamptz NOT NULL,
    heartbeat_at timestamptz NOT NULL,
    source_alive_at timestamptz NOT NULL,
    ended_at timestamptz,
    end_reason varchar(64)
);
CREATE INDEX collection_session_channel_time ON collection_session(channel_id, connected_at);
CREATE TABLE collection_loss (
    loss_id uuid PRIMARY KEY,
    channel_id varchar(32) NOT NULL,
    start_at timestamptz NOT NULL,
    end_at timestamptz NOT NULL,
    reason varchar(64) NOT NULL,
    CHECK (end_at > start_at)
);
CREATE INDEX collection_loss_channel_time ON collection_loss(channel_id, start_at);
CREATE TABLE collector_channel_buffer (
    channel_id varchar(32) PRIMARY KEY,
    collector_id uuid NOT NULL,
    observed_at timestamptz NOT NULL,
    oldest_unacked_at timestamptz,
    buffer_count int NOT NULL CHECK (buffer_count >= 0)
);
CREATE TABLE projection_observation (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    partition int NOT NULL,
    observed_at timestamptz NOT NULL,
    end_offset bigint NOT NULL,
    last_caught_up_at timestamptz,
    poll_heartbeat_at timestamptz,
    blocked boolean NOT NULL,
    PRIMARY KEY (projection, run_id, topic, partition)
);
CREATE TABLE ranking_snapshot (
    snapshot_id uuid PRIMARY KEY,
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    window_sec int NOT NULL,
    evaluated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    algorithm_version varchar(40) NOT NULL,
    UNIQUE (projection, run_id, window_sec, evaluated_at, algorithm_version)
);
CREATE TABLE ranking_entry (
    snapshot_id uuid NOT NULL REFERENCES ranking_snapshot(snapshot_id),
    channel_id varchar(32) NOT NULL,
    rank int,
    count bigint,
    reference_count bigint NOT NULL,
    coverage double precision NOT NULL,
    status varchar(32) NOT NULL,
    reasons jsonb NOT NULL,
    previous_rank int,
    previous_reason varchar(40),
    PRIMARY KEY (snapshot_id, channel_id)
);
CREATE TABLE snapshot_quality_revision (
    snapshot_id uuid NOT NULL,
    channel_id varchar(32) NOT NULL,
    reason varchar(64) NOT NULL,
    revised_at timestamptz NOT NULL,
    PRIMARY KEY (snapshot_id, channel_id),
    FOREIGN KEY (snapshot_id, channel_id) REFERENCES ranking_entry(snapshot_id, channel_id)
);
CREATE TABLE channel_alert_state (
    run_id varchar(80) NOT NULL,
    channel_id varchar(32) NOT NULL,
    algorithm_version varchar(40) NOT NULL,
    last_evaluated_at timestamptz NOT NULL,
    consecutive_hits int NOT NULL,
    last_alert_at timestamptz,
    PRIMARY KEY (run_id, channel_id, algorithm_version)
);
CREATE TABLE spike_event (
    spike_id uuid PRIMARY KEY,
    run_id varchar(80) NOT NULL,
    channel_id varchar(32) NOT NULL,
    evaluated_at timestamptz NOT NULL,
    algorithm_version varchar(40) NOT NULL,
    count bigint NOT NULL,
    baseline numeric NOT NULL,
    ratio numeric NOT NULL,
    quality_warning varchar(64),
    UNIQUE (run_id, channel_id, evaluated_at, algorithm_version)
);
CREATE INDEX spike_feed ON spike_event(run_id, evaluated_at DESC, spike_id DESC);
