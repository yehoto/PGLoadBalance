#!/bin/bash
set -e

# Этот скрипт выполняется только один раз при первом запуске primary

echo "*************** Configuring Primary Server ***************"

# Создаем пользователя с правом репликации (если еще не создан)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER replicator_user WITH REPLICATION ENCRYPTED PASSWORD '$POSTGRES_PASSWORD';
    -- Создаем тестовую таблицу (опционально)
    CREATE TABLE IF NOT EXISTS test_replication (id SERIAL PRIMARY KEY, data TEXT);
EOSQL

# Настраиваем postgresql.conf
cat >> ${PGDATA}/postgresql.conf <<EOF

# Custom config for replication
listen_addresses = '*'
wal_level = replica
max_wal_senders = 3
wal_keep_size = 512MB
archive_mode = on
archive_command = 'cp %p /var/lib/postgresql/data/archive/%f'
synchronous_commit = on
synchronous_standby_names = '"postgres-replica"'
EOF

# Создаем каталог для архивации WAL
mkdir -p ${PGDATA}/archive

# Настраиваем pg_hba.conf
cat >> ${PGDATA}/pg_hba.conf <<EOF

# Replication settings
host    replication     replicator_user    samenet    md5
host    all             all                samenet    md5
EOF

echo "*************** Primary Server Configuration Complete ***************"