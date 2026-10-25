CREATE TABLE report_source (run_id varchar(80) NOT NULL,topic varchar(249) NOT NULL,topic_id varchar(40) NOT NULL,PRIMARY KEY(run_id,topic));
CREATE TABLE report_session (
    run_id varchar(80) NOT NULL,report_id uuid NOT NULL,channel_id char(32) NOT NULL,
    anchor_observation_id uuid,observed_from timestamptz NOT NULL,observed_to timestamptz NOT NULL,ended_at timestamptz,
    payload jsonb NOT NULL,revision bigint NOT NULL DEFAULT 1,updated_at timestamptz NOT NULL,
    PRIMARY KEY(run_id,report_id)
);
CREATE INDEX report_session_channel ON report_session(run_id,channel_id,observed_to DESC);
CREATE INDEX report_session_expiry ON report_session(run_id,ended_at);
CREATE TABLE report_alias (run_id varchar(80) NOT NULL,alias_id uuid NOT NULL,canonical_id uuid NOT NULL,PRIMARY KEY(run_id,alias_id));
CREATE TABLE report_observation (
    run_id varchar(80) NOT NULL,event_id uuid NOT NULL,channel_id char(32) NOT NULL,observed_at timestamptz NOT NULL,
    ttl_seconds integer NOT NULL,live boolean NOT NULL,payload jsonb NOT NULL,payload_hash char(64) NOT NULL,report_id uuid,
    PRIMARY KEY(run_id,event_id)
);
CREATE INDEX report_observation_time ON report_observation(run_id,channel_id,observed_at DESC,event_id DESC);
CREATE INDEX report_observation_report ON report_observation(run_id,report_id);
CREATE TABLE report_event (
    run_id varchar(80) NOT NULL,event_id uuid NOT NULL,channel_id char(32) NOT NULL,event_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,payload_hash char(64) NOT NULL,late boolean NOT NULL,observation_id uuid,report_id uuid,
    PRIMARY KEY(run_id,event_id)
);
CREATE INDEX report_event_time ON report_event(run_id,channel_id,event_time);
CREATE INDEX report_event_observation ON report_event(run_id,observation_id);
CREATE INDEX report_event_report ON report_event(run_id,report_id);
CREATE TABLE report_bucket (
    run_id varchar(80) NOT NULL,report_id uuid NOT NULL,bucket_start timestamptz NOT NULL,
    message_count bigint NOT NULL CHECK(message_count>=0),PRIMARY KEY(run_id,report_id,bucket_start)
);
CREATE TABLE report_progress (
    run_id varchar(80) NOT NULL,topic varchar(249) NOT NULL,partition integer NOT NULL,next_offset bigint NOT NULL,updated_at timestamptz NOT NULL,
    PRIMARY KEY(run_id,topic,partition)
);
CREATE TABLE report_partition (
    run_id varchar(80) NOT NULL,topic varchar(249) NOT NULL,partition integer NOT NULL,observed_at timestamptz NOT NULL,poll_at timestamptz,
    source_end_offset bigint NOT NULL,caught_up_at timestamptz,blocked boolean NOT NULL,PRIMARY KEY(run_id,topic,partition)
);
