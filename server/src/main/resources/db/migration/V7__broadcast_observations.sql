CREATE TABLE broadcast_observation (
    run_id varchar(80) NOT NULL,
    event_id uuid NOT NULL,
    channel_id char(32) NOT NULL,
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (run_id, event_id)
);
CREATE INDEX observation_timeline ON broadcast_observation(run_id,channel_id,observed_at,event_id);
CREATE INDEX observation_retention ON broadcast_observation(observed_at);
CREATE TABLE observation_progress (
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    partition integer NOT NULL,
    next_offset bigint NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (run_id,topic,partition)
);
