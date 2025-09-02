#!/bin/bash
set -e

echo "*************** Configuring Primary Server ***************"


psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    -- Создаем пользователя репликации С ПРАВАМИ СУПЕРПОЛЬЗОВАТЕЛЯ
    -- ВНИМАНИЕ: Это небезопасно для продакшена!
    CREATE USER $REPLICATOR_USER WITH REPLICATION SUPERUSER ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
EOSQL


echo "Creating replication slot 'replica1_slot'..."
psql -v ON_ERROR_STOP=1 --username "postgres" --dbname "postgres" <<-EOSQL
    SELECT pg_create_physical_replication_slot('replica1_slot');
EOSQL

cat >> ${PGDATA}/postgresql.conf <<EOF

# --- Replication config ---
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_wal_size=300MB
wal_keep_size=0
synchronous_commit = on
synchronous_standby_names = 'replica1'
# --------------------------
EOF

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