# PGLoadBalance

Генератор нагрузки для PostgreSQL, который поддерживает объем базы данных в заданных пределах.

## Prometheus - просмотр метрик
URL: http://localhost:9090

## просматривать логи в режиме реального времени
docker logs -f docker-loadgen-1

## запуск и остановка контейнеров
docker-compose up -d
docker volume rm pg_logic_replica_data
docker volume rm pg_phys_replica_data
docker volume rm pg_master_data
docker volume create pg_logic_replica_data
docker volume create pg_master_data
docker volume create pg_phys_replica_data
docker-compose down -v