-- 새 테이블과 backfill 반영 뒤 프로세스를 종료하는 리허설 전용이다.
SET LOCAL application_name = 'migration-interruption-fixture';
SET LOCAL client_connection_check_interval = '1s';
CREATE TABLE release_migration_probe (
    projection varchar(100) NOT NULL,
    run_id varchar(80) NOT NULL,
    event_count bigint NOT NULL,
    PRIMARY KEY (projection,run_id)
);
INSERT INTO release_migration_probe
SELECT projection,run_id,sum(chat_count) FROM chat_bucket_10s GROUP BY projection,run_id;
DO $$ BEGIN
    IF (SELECT count(*) FROM release_migration_probe WHERE event_count=10) IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION USING ERRCODE='P1301', MESSAGE='SYNTHETIC_BACKFILL_NOT_VISIBLE';
    END IF;
    PERFORM nextval('release_failure_attempts');
    PERFORM pg_sleep(120);
END $$;
