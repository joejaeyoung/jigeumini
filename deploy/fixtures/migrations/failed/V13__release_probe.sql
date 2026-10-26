-- DDL과 backfill을 같은 트랜잭션에서 확인한 뒤 고정 실패를 발생시킨다.
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
    RAISE EXCEPTION USING ERRCODE='P1300', MESSAGE='SYNTHETIC_MIGRATION_FAILURE';
END $$;
