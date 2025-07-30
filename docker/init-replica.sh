#!/bin/bash
set -e

# Этот скрипт выполняется при первом запуске реплики

# Функция ожидания готовности primary
wait_for_primary() {
    echo "Waiting for primary database to be ready..."
    until pg_isready -h postgres-primary -p 5432 -U "$POSTGRES_USER"; do
        echo "Still waiting for primary..."
        sleep 2
    done
    echo "Primary database is ready!"
}

# Проверяем, существует ли уже кластер данных
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "*************** Initializing Replica from Primary ***************"
    
    wait_for_primary

    echo "Starting base backup..."
    # Выполняем базовый бэкап с primary
    pg_basebackup -h postgres-primary -D "$PGDATA" -U "$POSTGRES_USER" -vP --wal-method=stream --write-recovery-conf

    # Создаем файл standby.signal для указания, что это standby сервер (для новых версий)
    touch "$PGDATA/standby.signal"

    # Настраиваем параметры восстановления (для новых версий они идут в postgresql.auto.conf или standby.signal)
    # Но мы можем явно указать их через команду запуска в docker-compose.yml
    # или добавить в postgresql.conf
    cat >> "$PGDATA/postgresql.conf" <<-EOL

# Recovery settings added by init-replica.sh
primary_conninfo = 'host=postgres-primary port=5432 user=$POSTGRES_USER password=$POSTGRES_PASSWORD application_name=postgres-replica'
restore_command = 'cp /var/lib/postgresql/data/archive/%f %p'
recovery_target_timeline = 'latest'
EOL

    echo "*************** Replica Initialization Complete ***************"
else
    echo "*************** Replica data directory already exists, skipping base backup ***************"
fi