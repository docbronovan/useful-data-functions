-- Find all running processes with:
CREATE OR REPLACE VIEW public.active_locks AS 
 SELECT t.schemaname,
    t.relname,
    l.locktype,
    l.page,
    l.virtualtransaction,
    l.pid,
    l.mode,
    l.granted
   FROM pg_locks l
   JOIN pg_stat_all_tables t ON l.relation = t.relid
  WHERE t.schemaname <> 'pg_toast'::name AND t.schemaname <> 'pg_catalog'::name
  ORDER BY t.schemaname, t.relname;
SELECT * FROM active_locks;

-- and kill it with:
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid in (<lock-processes-from-above>);


-- Grant schema/table access to user
GRANT CONNECT ON DATABASE mydb TO xxx;
-- This assumes you're actually connected to mydb..
GRANT USAGE ON SCHEMA public TO xxx;
GRANT SELECT ON mytable TO xxx;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO xxx;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
   GRANT SELECT ON TABLES TO xxx;

-- List all TABLES
SELECT table_schema,table_name
FROM information_schema.tables
WHERE table_schema <> 'information_schema'
AND table_schema <> 'pg_catalog'
ORDER BY table_schema,table_name;

-- List current users and database
select current_user;

select current_database();

-- avoid division by zero
NULLIF(column_name,0)

-- Delete duplicate rows when you dont have unique ids
DELETE FROM dupes a
WHERE a.ctid <> (SELECT min(b.ctid)
                 FROM   dupes b
                 WHERE  a.key = b.key);
