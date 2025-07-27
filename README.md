# PGLoadBalance

Генератор нагрузки для PostgreSQL, который поддерживает объем базы данных в заданных пределах.

## Prometheus - просмотр метрик
URL: http://localhost:9090

## Grafana - дашборды
URL: http://localhost:3000
- Логин: admin
- пароль: admin

Home -> Connections -> add data source (Prometheus: http://prometheus:9090)

Пример дашборда для импорта: PostgreSQL Database Dashboard: 9628

## просматривать логи в режиме реального времени
docker logs -f docker-loadgen-1

## запуск и остановка контейнеров
- docker volume rm pg_logic_replica_data
- docker volume rm pg_phys_replica_data
- docker volume rm pg_master_data
- docker volume rm grafana_data
- docker volume create grafana_data
- docker volume create pg_logic_replica_data
- docker volume create pg_master_data
- docker volume create pg_phys_replica_data
- docker-compose up -d ( docker-compose up -d --build )
- docker-compose down -v

docker volume rm pg_logic_replica_data
docker volume rm pg_phys_replica_data
docker volume rm pg_master_data
docker volume rm grafana_data
docker volume create grafana_data
docker volume create pg_logic_replica_data
docker volume create pg_master_data
docker volume create pg_phys_replica_data