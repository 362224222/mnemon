FROM golang:1.24.6-alpine3.22 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mnemon-harness ./cmd/mnemon-harness && \
    CGO_ENABLED=0 go build -trimpath -o /out/mnemond ./cmd/mnemond

FROM alpine:3.22

RUN adduser -D -u 10001 agent && mkdir -p /workspace && chown agent:agent /workspace
COPY --from=build --chown=10001:10001 /out/mnemon-harness /usr/local/bin/mnemon-harness
COPY --from=build --chown=10001:10001 /out/mnemond /usr/local/bin/mnemond

USER agent
WORKDIR /workspace
CMD ["sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"]
