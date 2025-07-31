#!/bin/bash
set -e

echo "*************** Configuring Primary Server ***************"

# Создаем пользователя и базу данных
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    -- Создаем базу данных, если она еще не существует
    SELECT 'CREATE DATABASE appdb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'appdb');
    -- Создаем пользователя репликации С ПРАВАМИ СУПЕРПОЛЬЗОВАТЕЛЯ
    -- ВНИМАНИЕ: Это небезопасно для продакшена!
    CREATE USER $REPLICATOR_USER WITH REPLICATION SUPERUSER ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
    -- Предоставляем привилегии для работы с БД appdb (теперь избыточно, так как superuser)
    -- GRANT ALL PRIVILEGES ON DATABASE appdb TO $REPLICATOR_USER;
    -- Назначаем роль pg_monitor для доступа к функциям мониторинга (полезно, но не обязательно для loadgen)
    -- GRANT pg_monitor TO $REPLICATOR_USER;
EOSQL

# --- Добавляем создание репликационного слота ---
echo "Creating replication slot 'replica1_slot'..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    SELECT 'CREATE_REPLICATION_SLOT replica1_slot PHYSICAL' WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica1_slot');
    -- Или просто: SELECT pg_create_physical_replication_slot('replica1_slot');
    -- Второй вариант проще и не требует проверки существования
    -- Выберем второй вариант:
EOSQL

psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    SELECT pg_create_physical_replication_slot('replica1_slot');
EOSQL
# --- Конец добавления ---

# Настраиваем postgresql.conf для репликации
cat >> ${PGDATA}/postgresql.conf <<EOF

# --- Replication config ---
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
synchronous_commit = on
synchronous_standby_names = 'replica1'
# --------------------------
EOF

# Настраиваем pg_hba.conf для разрешения подключений
cat > ${PGDATA}/pg_hba.conf <<EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# Разрешить локальные подключения через trust (для внутренних операций)
local   all             all                                     trust

# Разрешить все подключения к любым БД через trust (для тестирования)
host    all             all             all                     trust

# ЯВНО разрешить репликацию для пользователя replicator с любого адреса через scram-sha-256
host    replication     $REPLICATOR_USER  all                   scram-sha-256
EOF

echo "*************** Primary Configuration Complete ***************"