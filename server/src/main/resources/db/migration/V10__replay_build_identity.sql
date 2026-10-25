ALTER TABLE replay_run ADD COLUMN created_build varchar(64) NOT NULL DEFAULT 'legacy-unknown';
ALTER TABLE replay_run ADD COLUMN worker_build varchar(64);
