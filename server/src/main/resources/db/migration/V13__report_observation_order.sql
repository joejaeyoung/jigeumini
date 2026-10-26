CREATE INDEX report_observation_report_time
    ON report_observation(run_id,report_id,observed_at DESC,event_id DESC);
DROP INDEX report_observation_report;
