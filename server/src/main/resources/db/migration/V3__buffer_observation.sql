-- 창 종료 이후의 첫 drain 표본을 이후 flush가 덮어쓰지 않도록 짧게 보존한다.
CREATE TABLE collector_buffer_observation (
    channel_id varchar(32) NOT NULL,
    collector_id uuid NOT NULL,
    observed_at timestamptz NOT NULL,
    oldest_unacked_at timestamptz,
    PRIMARY KEY (channel_id, collector_id, observed_at)
);
CREATE INDEX collector_buffer_observation_time ON collector_buffer_observation(observed_at);
