FROM golang:1.21-alpine AS build

RUN apk add --no-cache git ca-certificates

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/quote-service ./main.go

FROM alpine:3.18

RUN apk add --no-cache ca-certificates tzdata

COPY --from=build /bin/quote-service /bin/quote-service

EXPOSE 8002

ENTRYPOINT ["/bin/quote-service"]
