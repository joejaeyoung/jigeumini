CREATE TABLE replay_run (
    run_id uuid PRIMARY KEY,
    manifest jsonb NOT NULL,
    state varchar(12) NOT NULL CHECK (state IN ('QUEUED','RUNNING','COMPLETED','FAILED','CANCELLED')),
    owner_id uuid,
    cancel_requested boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    heartbeat_at timestamptz,
    finished_at timestamptz,
    read_count bigint NOT NULL DEFAULT 0 CHECK (read_count>=0),
    filtered_count bigint NOT NULL DEFAULT 0 CHECK (filtered_count>=0),
    unique_count bigint NOT NULL DEFAULT 0 CHECK (unique_count>=0),
    duplicate_count bigint NOT NULL DEFAULT 0 CHECK (duplicate_count>=0),
    error_count bigint NOT NULL DEFAULT 0,
    reason varchar(40)
);
CREATE INDEX replay_queue ON replay_run(created_at) WHERE state='QUEUED';
