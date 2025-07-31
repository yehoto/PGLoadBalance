-- -- init-master.sql
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE USER replicator WITH REPLICATION PASSWORD 'replicatorpass';
    RAISE NOTICE 'Replication user created';
  END IF;
END $$;
-- -- 1) Создание пользователя репликации (не обязательно при trust, но можно оставить)
-- DO $$
-- BEGIN
--   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
--     -- Создаем пользователя с правами репликации. Пароль не обязателен при trust.
--     CREATE USER replicator WITH REPLICATION; -- Убираем пароль
--     RAISE NOTICE 'Replication user ''replicator'' created.';
--   ELSE
--     RAISE NOTICE 'Replication user ''replicator'' already exists.';
--   END IF;
-- END
-- $$;

-- -- 2) Перезапись pg_hba.conf для разрешения ВСЕМУ с trust
-- -- Этот способ показан для примера. Лучше использовать volumes или COPY из файла.
-- DO $$
-- DECLARE
--   hba_path TEXT;
-- BEGIN
--   -- Получаем путь к data_directory
--   SHOW data_directory INTO hba_path;
--   hba_path := hba_path || '/pg_hba.conf';

--   -- Очищаем существующий файл и записываем новые правила
--   -- ВНИМАНИЕ: Это уничтожит существующие правила!
--   EXECUTE format('COPY (SELECT unnest(ARRAY[
--     ''# TYPE  DATABASE        USER            ADDRESS                 METHOD'',
--     ''local   all             all                                     trust'', -- trust для локальных соединений
--     ''host    all             all             127.0.0.1/32            trust'', -- trust для localhost
--     ''host    all             all             ::1/128                 trust'', -- trust для localhost IPv6
--     ''# Replication - trust для тестирования'',
--     ''host    replication     all             10.10.0.0/16             trust'', -- trust для репликации из новой сети replication_net
--     ''host    all             all             172.20.0.0/16           trust'',  -- trust для новой основной сети
--     ''host    all             all             0.0.0.0/0               trust''   -- trust для всех остальных (осторожно в продакшене!)
--   ])) TO %L', hba_path);

--   -- Перезагружаем конфигурацию
--   PERFORM pg_reload_conf();
--   RAISE NOTICE 'pg_hba.conf updated and reloaded.';
-- EXCEPTION WHEN OTHERS THEN
--   RAISE WARNING 'Failed to update pg_hba.conf: %', SQLERRM;
-- END
-- $$;

-- -- 3) Создание репликационных слотов (опционально, так как pg_basebackup может создать)
-- -- Убираем блок создания слота, так как pg_basebackup делает это с -R

-- -- 4) Установка начальных параметров синхронной репликации
-- -- Эти параметры уже заданы в командной строке docker-compose
-- ALTER SYSTEM SET synchronous_commit = 'on';
-- ALTER SYSTEM SET synchronous_standby_names = '1 (repl_phys_replica)';

-- -- 5) Предоставление прав (не нужно для физической репликации с trust)
-- -- Убираем

-- -- 6) Финальные действия
-- -- Выполняем обслуживание
-- VACUUM ANALYZE;
-- RAISE NOTICE 'Initialization script completed.';
-- -- Примечание: pg_reload_conf() вызывается автоматически при завершении скрипта инициализации.