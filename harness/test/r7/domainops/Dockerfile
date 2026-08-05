FROM golang:1.24.6-alpine3.22 AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/domain-world ./testdata/r7/domain-ops/cmd/domain-world && \
    CGO_ENABLED=0 go build -trimpath -o /out/domain-load ./testdata/r7/domain-ops/cmd/domain-load && \
    CGO_ENABLED=0 go build -trimpath -o /out/domainctl ./testdata/r7/domain-ops/cmd/domainctl && \
    CGO_ENABLED=0 go build -trimpath -o /out/mnemon-harness ./cmd/mnemon-harness && \
    CGO_ENABLED=0 go build -trimpath -o /out/mnemond ./cmd/mnemond

FROM alpine:3.22 AS world

RUN adduser -D -u 10001 operator
COPY --from=build --chown=10001:10001 /out/domain-world /usr/local/bin/domain-world
COPY --from=build --chown=10001:10001 /out/domain-load /usr/local/bin/domain-load
COPY --from=build --chown=10001:10001 /out/domainctl /usr/local/bin/domainctl

USER operator
ENTRYPOINT ["domain-world"]

FROM node:22-alpine3.22 AS agent

ARG PI_VERSION=0.83.0
RUN apk add --no-cache bash ca-certificates jq && \
    npm install -g --ignore-scripts --no-audit --no-fund \
      "@earendil-works/pi-coding-agent@${PI_VERSION}" && \
    adduser -D -u 10001 agent && mkdir -p /workspace && chown agent:agent /workspace
COPY --from=build --chown=10001:10001 /out/domainctl /usr/local/bin/domainctl
COPY --from=build --chown=10001:10001 /out/mnemon-harness /usr/local/bin/mnemon-harness
COPY --from=build --chown=10001:10001 /out/mnemond /usr/local/bin/mnemond

USER agent
WORKDIR /workspace
CMD ["sh", "-c", "trap 'exit 0' TERM INT; while :; do sleep 3600 & wait $!; done"]
