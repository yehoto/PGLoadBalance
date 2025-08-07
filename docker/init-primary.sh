#!/bin/bash
set -e
echo "*************** Configuring Primary Server for Logical Replication ***************"
# Создаем базу данных appdb
echo "Creating database appdb..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-'EOSQL'
    SELECT 'CREATE DATABASE appdb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'appdb') \gexec
EOSQL

# Создаем пользователя репликации с возможностью входа
echo "Creating replication user with LOGIN permission..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "appdb" <<-'EOSQL'
    DO $$
    BEGIN
        CREATE USER "replicator" WITH REPLICATION SUPERUSER LOGIN ENCRYPTED PASSWORD 'replicator_pass';
    EXCEPTION WHEN duplicate_object THEN
        -- Роль уже существует, меняем только свойства
        ALTER USER "replicator" WITH REPLICATION SUPERUSER LOGIN ENCRYPTED PASSWORD 'replicator_pass';
    END $$;
EOSQL

# Создаем таблицы до создания публикации
echo "Creating tables on primary..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "appdb" <<-'EOSQL'
CREATE TABLE IF NOT EXISTS test_0 (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS test_1 (
    id SERIAL PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
EOSQL

# Создаем логический слот репликации
echo "Creating logical replication slot..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "appdb" <<-'EOSQL'
    SELECT pg_create_logical_replication_slot('replica1_slot', 'pgoutput');
EOSQL

# Создаем публикацию для всех таблиц
echo "Creating publication..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "appdb" <<-'EOSQL'
    CREATE PUBLICATION appdb_publication FOR ALL TABLES;
EOSQL

# НАСТРАИВАЕМ ПОЛНЫЙ TRUST ДЛЯ ВСЕХ ПОДКЛЮЧЕНИЙ
echo "Configuring pg_hba.conf for FULL TRUST authentication..."
cat > ${PGDATA}/pg_hba.conf <<-'EOF'
# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   all             all                                     trust
host    all             all             127.0.0.1/32            trust
host    all             all             ::1/128                 trust
host    all             all             0.0.0.0/0               trust
host    replication     all             all                     trust
EOF

# Перезагружаем конфигурацию
pg_ctl -D "${PGDATA}" reload
echo "*************** Primary Configuration Complete ***************"