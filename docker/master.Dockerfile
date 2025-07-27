FROM postgres:17

# Установка pg_repack и pgstattuple
RUN apt-get update \
    && apt-get install -y postgresql-17-repack \
    && rm -rf /var/lib/apt/lists/*

    # RUN apt-get update \
    # && apt-get install -y postgresql-17-repack postgresql-17-pgstattuple \
    # && rm -rf /var/lib/apt/lists/*
# Копируем скрипты инициализации
COPY init-master.sql /docker-entrypoint-initdb.d/01_master_init.sql

# Оставляем стандартный ENTRYPOINT от образа postgres
