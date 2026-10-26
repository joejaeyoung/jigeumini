-- 이미지 복원 리허설 전용이다. 제품 migration 디렉터리에 넣지 않는다.
CREATE TABLE release_migration_probe (
    projection varchar(100) NOT NULL,
    run_id varchar(80) NOT NULL,
    event_count bigint NOT NULL,
    PRIMARY KEY (projection,run_id)
);
INSERT INTO release_migration_probe
SELECT projection,run_id,sum(chat_count) FROM chat_bucket_10s GROUP BY projection,run_id;
