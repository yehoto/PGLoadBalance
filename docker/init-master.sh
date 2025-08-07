#!/bin/bash
set -e

# Добавляем записи в pg_hba.conf ПОСЛЕ инициализации
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"
echo "host all all all md5" >> "$PGDATA/pg_hba.conf"

# Перезагружаем конфигурацию
pg_ctl reload -D "$PGDATA" > /dev/null 2>&1 || true

# Создаем репликационный слот ТОЛЬКО ЕСЛИ ЕГО НЕТ
if [ ! -f "$PGDATA/standby.signal" ]; then
  if ! psql -U postgres -tAc "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'phys_slot'" | grep -q 1; then
    echo "SELECT pg_create_physical_replication_slot('phys_slot');" | psql -U postgres
  fi
fi