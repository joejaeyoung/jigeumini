CREATE TABLE projection_source (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    topic_id varchar(24) NOT NULL,
    PRIMARY KEY (projection, run_id, topic)
);
CREATE TABLE projection_failure (
    projection varchar(40) NOT NULL,
    run_id varchar(80) NOT NULL,
    failure_id varchar(64) NOT NULL,
    source_topic varchar(249) NOT NULL,
    topic_id varchar(24) NOT NULL,
    source_partition int NOT NULL,
    source_offset bigint NOT NULL,
    error_code varchar(64) NOT NULL,
    channel_id varchar(32),
    affected_from timestamptz NOT NULL,
    affected_to timestamptz NOT NULL,
    quarantined_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (projection, run_id, failure_id),
    CHECK (affected_to > affected_from)
);
CREATE INDEX projection_failure_time ON projection_failure(run_id, source_topic, affected_to);
CREATE INDEX projection_failure_recent ON projection_failure(projection, run_id, quarantined_at DESC);
