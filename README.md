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
