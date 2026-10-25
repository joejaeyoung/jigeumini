#!/bin/sh
set -eu
# 이 파일은 새 로컬 리허설 DB에서만 실행된다. 실제 비밀은 파일로만 주입한다.
psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set ON_ERROR_STOP=1 --set runtime_secret="$(cat /run/secrets/runtime_password)" <<'SQL'
SELECT format('CREATE ROLE app_runtime LOGIN PASSWORD %L', :'runtime_secret') \gexec
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
SQL
