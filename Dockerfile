# syntax=docker/dockerfile:1

FROM golang:1.23 AS builder

WORKDIR /app

# Кэшируем зависимости
COPY go.mod go.sum ./
RUN go mod download

# Копируем остальной исходный код
COPY . .

# Сборка статического бинаря
RUN CGO_ENABLED=0 GOOS=linux go build -o loadgen ./cmd/loadgen

# Используем минимальный distroless-образ
FROM gcr.io/distroless/static

COPY --from=builder /app/loadgen /loadgen

# По умолчанию запускаем генератор; параметры можно
# переопределить в docker-compose через command: []
ENTRYPOINT ["/loadgen"] 