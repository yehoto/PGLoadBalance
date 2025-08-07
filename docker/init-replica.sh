#!/bin/bash
set -e
echo "*************** Configuring Replica for Logical Replication ***************"
# Ждем, пока primary будет полностью готов
echo "Waiting for primary to be fully initialized..."
for i in {1..60}; do
  if pg_isready -h postgres-primary -U postgres -d appdb > /dev/null 2>&1; then
    echo "Primary is ready!"
    break
  fi
  echo "Primary not ready yet. Warning... ($i/60)"
  sleep 2
  if [ $i -eq 60 ]; then
    echo "ERROR: Primary never became ready"
    exit 1
  fi
done

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

# Создаем таблицы (должны совпадать со структурой на primary)
echo "Creating tables on replica..."
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

echo "*************** Replica initialized, waiting for subscription creation ***************"