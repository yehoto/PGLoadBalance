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

-- -- 2) Создание публикации для логической репликации
-- \c postgres
-- DO $$
-- BEGIN
--   IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'my_publication') THEN
--     CREATE PUBLICATION my_publication FOR ALL TABLES;
--     RAISE NOTICE 'Publication created';
--   END IF;
-- END
-- $$;

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

-- 5) Установка расширений без изменения shared_preload_libraries
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pgstattuple') THEN
    CREATE EXTENSION pgstattuple;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_repack') THEN
    CREATE EXTENSION pg_repack;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements') THEN
    CREATE EXTENSION pg_stat_statements;
  END IF;
END
$$;

-- 6) Пример таблицы для генератора нагрузки
CREATE TABLE IF NOT EXISTS public.test_data (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    data TEXT,
    value FLOAT
);
CREATE INDEX IF NOT EXISTS idx_test_data_created ON public.test_data(created_at);
CREATE INDEX IF NOT EXISTS idx_test_data_value ON public.test_data(value);

-- 7) Права для пользователя репликатора
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT USAGE ON SCHEMA public TO replicator;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO replicator;

-- 8) Финальные опции: прямо в SQL, вне функций
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';
ALTER SYSTEM SET max_wal_size = '2GB';
ALTER SYSTEM SET checkpoint_timeout = '30min';

-- Применяем изменения и чистим
VACUUM ANALYZE;
SELECT pg_reload_conf();
