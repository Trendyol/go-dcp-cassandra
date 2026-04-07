FROM golang:1.25-alpine AS builder

WORKDIR /project

COPY . .

WORKDIR /project/example/simple

RUN go mod download
RUN CGO_ENABLED=0 go build -a -o example main.go

FROM alpine:3.21

WORKDIR /app

RUN apk --no-cache add ca-certificates

USER nobody
COPY --from=builder --chown=nobody:nobody /project/example/example .
COPY --from=builder --chown=nobody:nobody /project/example/config.yml ./config.yml

ENTRYPOINT ["./example"]