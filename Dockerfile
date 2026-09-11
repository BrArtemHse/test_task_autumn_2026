FROM golang:1.23.5-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kitchen-api ./cmd/kitchen-api && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/catalog-service ./cmd/catalog-service && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/order-service ./cmd/order-service && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/partner-service ./cmd/partner-service && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sample-restaurant-service ./cmd/sample-restaurant-service

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget && \
    addgroup -S app && \
    adduser -S -G app app

WORKDIR /app

COPY --from=build /out/ /usr/local/bin/
COPY --from=build /src/migrations/ ./migrations/

USER app
