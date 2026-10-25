#!/bin/sh
set -eu
# 새 로컬 리허설 DB만 대상. 비밀은 argv나 psql 변수 출력에 포함하지 않는다.
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 <<'SQL'
SET log_statement='none';
SET log_min_error_statement='panic';
SELECT format('CREATE ROLE app_runtime LOGIN PASSWORD %L', regexp_replace(pg_read_file('/run/secrets/runtime_password'), E'\\n$', '')) \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
