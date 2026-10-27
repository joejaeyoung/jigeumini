CREATE TABLE retention_window (
    scope varchar(80) NOT NULL,
    run_id varchar(80) NOT NULL,
    received_floor timestamptz,
    event_floor timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(scope,run_id)
);
CREATE INDEX processed_event_retention ON processed_event(projection,run_id,received_at,event_id);
ALTER TABLE replay_run ADD COLUMN result_expired_at timestamptz;
ALTER TABLE replay_run ADD COLUMN result_purged_at timestamptz;
CREATE INDEX replay_retention ON replay_run(finished_at,run_id)
    WHERE state IN ('COMPLETED','FAILED','CANCELLED');
CREATE TABLE retention_task (
    task varchar(80) NOT NULL,
    run_id varchar(80) NOT NULL,
    attempted_at timestamptz NOT NULL,
    succeeded_at timestamptz,
    state varchar(16) NOT NULL,
    deleted_rows bigint NOT NULL DEFAULT 0,
    duration_millis bigint NOT NULL DEFAULT 0,
    limited boolean NOT NULL DEFAULT false,
    failures bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(task,run_id)
);
CREATE INDEX category_observation_retention ON category_observation(run_id,observed_at,event_id);
CREATE INDEX notification_retention ON notification_event(run_id,evaluated_at,spike_id);
CREATE INDEX ranking_snapshot_retention ON ranking_snapshot(run_id,evaluated_at,snapshot_id);
CREATE INDEX collection_session_retention ON collection_session(ended_at) WHERE ended_at IS NOT NULL;
CREATE INDEX collection_loss_retention ON collection_loss(end_at);
