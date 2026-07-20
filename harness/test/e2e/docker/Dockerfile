# syntax=docker/dockerfile:1.7

ARG GO_IMAGE=golang:1.24.6-bookworm@sha256:ab1d1823abb55a9504d2e3e003b75b36dbeb1cbcc4c92593d85a84ee46becc6c

FROM ${GO_IMAGE} AS build
ARG GIT_SHA=unknown
ARG VERSION=r5-e2e
WORKDIR /src
COPY . .
RUN test "$(awk '$1 == "go" { print $2; exit }' go.mod)" = "1.24.6"
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/mnemon-harness ./harness/cmd/mnemon-harness && \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/mnemond ./harness/cmd/mnemond

FROM ${GO_IMAGE} AS runtime
ARG GIT_SHA=unknown
ARG SOURCE_TREE=unknown
ARG CODEX_VERSION=
ARG CODEX_PACKAGE_INTEGRITY=
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      io.mnemon.r5.source-tree="${SOURCE_TREE}" \
      io.mnemon.r5.image-purpose="docker-live-acceptance" \
      io.mnemon.r5.evidence-schema="1" \
      io.mnemon.r5.codex-version="${CODEX_VERSION}" \
      io.mnemon.r5.codex-integrity="${CODEX_PACKAGE_INTEGRITY}"

# The task oracles need the ordinary toolchains used by the five public
# fixtures. They deliberately do not install any Mnemon test backdoor.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates git jq nodejs npm procps tini util-linux && \
    rm -rf /var/lib/apt/lists/*

# A Live image is accepted only when both immutable Codex coordinates are
# supplied. Hermetic runs may build the same image without invoking Codex; the
# Live runner rejects such an image rather than silently installing "latest".
RUN set -eu; \
    if [ -n "${CODEX_VERSION}${CODEX_PACKAGE_INTEGRITY}" ]; then \
      test -n "${CODEX_VERSION}"; \
      test -n "${CODEX_PACKAGE_INTEGRITY}"; \
      observed="$(npm view "@openai/codex@${CODEX_VERSION}" dist.integrity)"; \
      test "${observed}" = "${CODEX_PACKAGE_INTEGRITY}"; \
      npm install --global --ignore-scripts "@openai/codex@${CODEX_VERSION}"; \
      codex --version >/dev/null; \
    fi

COPY --from=build /out/mnemon-harness /usr/local/bin/mnemon-harness
COPY --from=build /out/mnemond /usr/local/bin/mnemond
COPY harness/test/e2e/docker/idle.sh /opt/r5/bin/idle
COPY harness/test/e2e/docker/scripted-codex.sh /opt/r5/scripted/bin/codex
COPY harness/test/e2e/docker/scripted-agent-turn.sh /opt/r5/bin/scripted-agent-turn
COPY harness/test/e2e/docker/live-codex-exec.sh /opt/r5/bin/live-codex-exec
COPY harness/test/e2e/docker/live-init.sh /opt/r5/bin/live-init

RUN groupadd --gid 10001 r5 && \
    useradd --uid 10001 --gid 10001 --create-home --home-dir /home/r5 r5 && \
    chmod 0755 /opt/r5/bin/* /opt/r5/scripted/bin/codex && \
    install -d -o r5 -g r5 -m 0700 /workspace /workspace/.mnemon /home/r5/.codex /evidence

USER 10001:10001
WORKDIR /workspace
ENV HOME=/home/r5 CODEX_HOME=/home/r5/.codex
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/r5/bin/idle"]
