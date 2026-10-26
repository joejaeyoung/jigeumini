"""격리 DB 진단. 정해진 분류와 숫자만 반환하며 SQL 텍스트·바인드 값은 내보내지 않는다."""

TABLES=('category_event','category_progress','category_observation','category_bucket','category_revision',
        'report_event','report_progress','report_observation','report_session','report_bucket','report_alias',
        'processed_event','channel_bucket','trend_bucket','broadcast_observation','notification_event','spike_outbox')

def statements():
    family='CASE '+''.join("WHEN query ~* '\\m"+table+"\\M' THEN '"+table.upper()+"' " for table in TABLES)+"ELSE 'OTHER' END"
    return """SELECT coalesce(json_agg(row_to_json(s)),'[]'::json) FROM (
      SELECT queryid::text AS query_id,md5(query) AS template_hash,"""+family+""" AS family,
        CASE WHEN query ~* '^\\s*SELECT\\s+count\\(' THEN 'COUNT'
             WHEN query ~* '^\\s*INSERT' THEN 'INSERT'
             WHEN query ~* '^\\s*UPDATE' THEN 'UPDATE'
             WHEN query ~* '^\\s*DELETE' THEN 'DELETE'
             WHEN query ~* '^\\s*WITH' THEN 'CTE'
             WHEN query ~* '^\\s*SELECT' THEN 'SELECT' ELSE 'OTHER' END AS operation,
        calls,total_exec_time AS execution_millis,max_exec_time AS maximum_execution_millis,rows,
        shared_blks_hit,shared_blks_read,shared_blks_dirtied,temp_blks_read,temp_blks_written,wal_bytes
      FROM pg_stat_statements
      WHERE userid=(SELECT oid FROM pg_roles WHERE rolname='app_runtime') AND dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
      ORDER BY total_exec_time DESC,queryid
    ) s"""

ACTIVITY="""SELECT coalesce(json_agg(row_to_json(s)),'[]'::json) FROM (
    SELECT CASE application_name WHEN 'projection-probe' THEN 'PROBE' ELSE 'PRODUCT' END AS role,
      state,wait_event_type,wait_event,count(*) AS sessions,
      max(extract(epoch FROM clock_timestamp()-xact_start)*1000) AS oldest_transaction_millis,
      max(extract(epoch FROM clock_timestamp()-query_start)*1000) FILTER (WHERE state='active') AS oldest_active_millis
    FROM pg_stat_activity WHERE datname=current_database() AND usename='app_runtime'
    GROUP BY 1,state,wait_event_type,wait_event ORDER BY 1,state,wait_event_type,wait_event
  ) s"""
