#!/bin/bash
set -e

# Проверяем, существует ли уже кластер данных
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "*************** Initializing replica from primary ***************"
    # Удаляем содержимое PGDATA, если оно существует (например, из предыдущих запусков)
    rm -rf "$PGDATA"/*
    # Создаем архивный каталог, если он нужен для восстановления
    mkdir -p /var/lib/postgresql/archive

    # Ожидание готовности primary
    until pg_isready -h postgres-primary -p 5432 -U "$POSTGRES_USER"; do
        echo "Waiting for primary to be ready..."
        sleep 2
    done

    echo "Primary is ready. Starting base backup..."

    # Выполняем базовый бэкап с primary
    pg_basebackup -h postgres-primary -D "$PGDATA" -U "$POSTGRES_USER" -vP -W --wal-method=stream

    # Создаем файл standby.signal для указания, что это standby сервер
    touch "$PGDATA/standby.signal"

    # Создаем recovery.conf или recovery.signal + postgresql.auto.conf для новых версий
    # В PostgreSQL 12+ recovery.conf больше не используется, настройки идут в postgresql.auto.conf
    # Но мы можем использовать команду в postgresql.conf

    # Записываем параметры восстановления в postgresql.auto.conf или используем команду запуска
    # Мы будем использовать команду запуска в docker-compose, но на всякий случай:
    cat >> "$PGDATA/postgresql.conf" <<-EOL

# Recovery settings added by replica-entrypoint.sh
primary_conninfo = 'host=postgres-primary port=5432 user=$POSTGRES_USER password=$POSTGRES_PASSWORD application_name=postgres-replica'
restore_command = 'cp /var/lib/postgresql/archive/%f %p'
# archive_cleanup_command = 'pg_archivecleanup /var/lib/postgresql/archive %r' # Опционально, для очистки архивов
recovery_target_timeline = 'latest'
EOL

    echo "*************** Replica initialization complete ***************"
else
    echo "*************** Replica data directory already exists, skipping base backup ***************"
fi

# Скрипт завершается, и основной процесс postgres запускается через CMD docker-compose