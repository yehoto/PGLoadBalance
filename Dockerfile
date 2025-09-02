FROM golang:1.23 AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o loadgen ./cmd/loadgen

FROM postgres:17

RUN apt-get update \
    && apt-get install -y postgresql-17-repack \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/loadgen /loadgen

ENTRYPOINT ["/loadgen"]
