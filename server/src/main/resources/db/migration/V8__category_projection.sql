CREATE TABLE category_source (
    run_id varchar(80) NOT NULL, topic varchar(249) NOT NULL, topic_id varchar(40) NOT NULL,
    PRIMARY KEY(run_id,topic)
);
CREATE TABLE category_observation (
    run_id varchar(80) NOT NULL, event_id uuid NOT NULL, channel_id char(32) NOT NULL,
    observed_at timestamptz NOT NULL, ttl_seconds integer NOT NULL,
    category_key varchar(64) NOT NULL, category_name varchar(120), payload_hash char(64) NOT NULL,
    PRIMARY KEY(run_id,event_id)
);
CREATE INDEX category_observation_time ON category_observation(run_id,channel_id,observed_at DESC,event_id DESC);
CREATE TABLE category_event (
    run_id varchar(80) NOT NULL, event_id uuid NOT NULL, channel_id char(32) NOT NULL,
    event_time timestamptz NOT NULL, payload_hash char(64) NOT NULL,
    category_key varchar(64) NOT NULL, revision bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(run_id,event_id)
);
CREATE INDEX category_event_time ON category_event(run_id,channel_id,event_time);
CREATE TABLE category_bucket (
    run_id varchar(80) NOT NULL, category_key varchar(64) NOT NULL, channel_id char(32) NOT NULL,
    bucket_start timestamptz NOT NULL, message_count bigint NOT NULL CHECK(message_count >= 0),
    PRIMARY KEY(run_id,category_key,channel_id,bucket_start)
);
CREATE INDEX category_bucket_time ON category_bucket(run_id,bucket_start);
CREATE TABLE category_revision (run_id varchar(80) PRIMARY KEY, revision bigint NOT NULL);
CREATE TABLE category_progress (
    run_id varchar(80) NOT NULL, topic varchar(249) NOT NULL, partition integer NOT NULL,
    next_offset bigint NOT NULL, updated_at timestamptz NOT NULL,
    PRIMARY KEY(run_id,topic,partition)
);
CREATE TABLE category_partition (
    run_id varchar(80) NOT NULL, topic varchar(249) NOT NULL, partition integer NOT NULL,
    observed_at timestamptz NOT NULL, poll_at timestamptz,
    source_end_offset bigint NOT NULL, caught_up_at timestamptz, blocked boolean NOT NULL,
    PRIMARY KEY(run_id,topic,partition)
);
