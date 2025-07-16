FROM golang:1.13.3-alpine3.10 AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -o server

FROM alpine:3.10
RUN apk add --no-cache ca-certificates

COPY --from=builder /app/server /server
COPY templates /templates
COPY nuget-server-config-local.json /

EXPOSE 80
CMD ["/server"]
