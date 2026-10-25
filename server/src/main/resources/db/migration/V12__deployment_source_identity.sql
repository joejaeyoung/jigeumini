CREATE TABLE deployment_source_identity (
    run_id varchar(80) NOT NULL,
    topic varchar(249) NOT NULL,
    cluster_id varchar(128) NOT NULL,
    topic_id varchar(128) NOT NULL,
    partitions int NOT NULL CHECK (partitions > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id,topic)
);
