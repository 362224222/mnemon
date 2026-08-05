FROM golang:1.24.6-alpine3.22 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/mnemon-harness ./cmd/mnemon-harness && \
    CGO_ENABLED=0 go build -trimpath -o /out/mnemond ./cmd/mnemond && \
    CGO_ENABLED=0 go build -trimpath -o /out/r8-peer ./internal/selector/testdata/network/cmd/r8-peer

FROM alpine:3.22

RUN adduser -D -u 10001 agent && mkdir -p /workspace && chown agent:agent /workspace
COPY --from=build --chown=10001:10001 /out/mnemon-harness /usr/local/bin/mnemon-harness
COPY --from=build --chown=10001:10001 /out/mnemond /usr/local/bin/mnemond
COPY --from=build --chown=10001:10001 /out/r8-peer /usr/local/bin/r8-peer
COPY --chown=10001:10001 test/r8/network/docker/entrypoint.sh /usr/local/bin/r8-entrypoint
RUN chmod 0755 /usr/local/bin/r8-entrypoint

USER agent
WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/r8-entrypoint"]
