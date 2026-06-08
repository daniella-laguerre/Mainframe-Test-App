FROM golang:1.21-alpine AS build

RUN apk add --no-cache gcc musl-dev

WORKDIR /app

COPY go.mod ./
COPY go.sum* ./
RUN go mod download || true

COPY . .

RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /market-data-sim main.go

FROM alpine:3.18

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /market-data-sim /market-data-sim

ENTRYPOINT ["/market-data-sim"]
