-- Создание пользователя репликации
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicatorpass';
ALTER USER replicator WITH SUPERUSER; -- Для упрощения

-- УДАЛЕНА СТРОКА СОЗДАНИЯ СЛОТА, ТАК КАК ОНА ВЫПОЛНЯЕТСЯ В init-master.sh
-- SELECT pg_create_physical_replication_slot('phys_slot');

-- Финальные опции
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET pg_stat_statements.track = 'all';
ALTER SYSTEM SET max_wal_size = '1GB';
ALTER SYSTEM SET checkpoint_timeout = '15min';

-- Применяем изменения
SELECT pg_reload_conf();