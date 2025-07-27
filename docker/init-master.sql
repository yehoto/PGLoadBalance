-- init-master.sql

-- 1) Создание пользователя репликации
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
    CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';
    ALTER USER replicator WITH SUPERUSER;
    RAISE NOTICE 'Replication user created';
  END IF;
END
$$;

-- 3) Перезапись pg_hba.conf для разрешения репликации
DO $$
DECLARE
  hba_path TEXT;
BEGIN
  SHOW data_directory INTO hba_path;
  hba_path := hba_path || '/pg_hba.conf';
  EXECUTE format('COPY (SELECT unnest(ARRAY[
    ''local   all             all            trust'',
    ''host    replication      replicator     10.5.0.0/16 scram-sha-256'',
    ''host    all              all            172.18.0.0/16 scram-sha-256''
  ])) TO %L', hba_path);
  PERFORM pg_reload_conf();
  RAISE NOTICE 'pg_hba.conf updated and reloaded';
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'Failed to update pg_hba.conf: %', SQLERRM;
END
$$;

-- 4) Создание репликационных слотов
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'phys_slot') THEN
    PERFORM pg_create_physical_replication_slot('phys_slot', true);
    RAISE NOTICE 'Physical replication slot created';
  END IF;
END
$$;

-- 7) Права для пользователя репликатора
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO replicator;

-- 8) Финальные опции: 
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';
ALTER SYSTEM SET max_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';

-- Применяем изменения и чистим
VACUUM ANALYZE;
SELECT pg_reload_conf();
