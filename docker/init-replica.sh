#!/bin/bash
set -e

echo "*************** Initializing Replica ***************"

# Ждем, пока primary будет готов принимать подключения
sleep 10

# Удаляем существующие данные реплики
echo "Cleaning old data..."
rm -rf ${PGDATA:?}/*

# Выполняем базовый бэкап с primary
echo "Starting base backup from primary..."
PGPASSWORD=${REPLICATOR_PASSWORD} pg_basebackup -h postgres-primary -D ${PGDATA} -U ${REPLICATOR_USER} --verbose --progress --wal-method=stream

# Настраиваем подключение к primary в файле postgresql.conf реплики
echo "Configuring replica connection to primary..."
cat >> ${PGDATA}/postgresql.conf <<EOF

# --- Standby Configuration ---
primary_conninfo = 'host=postgres-primary port=5432 user=${REPLICATOR_USER} password=${REPLICATOR_PASSWORD} application_name=replica1'
# primary_slot_name = 'replica1_slot' # Убираем на время тестирования
hot_standby = on
# -----------------------------
EOF

# Создаем файл standby.signal
echo "Creating standby.signal..."
touch ${PGDATA}/standby.signal

echo "*************** Replica Initialized ***************"