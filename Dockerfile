# syntax=docker/dockerfile:labs

# 1. Build go2rtc binary
ARG GO_VERSION="1.25"

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}

WORKDIR /build

RUN apk add git

# Cache dependencies
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/root/.cache/go-build go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -ldflags "-s -w" -trimpath


# 2. Download ffmpeg-homebridge (has libfdk_aac for HomeKit backchannel audio)
FROM alpine:3.21 AS ffmpeg-homebridge
ARG TARGETARCH

RUN apk add --no-cache curl tar
RUN if [ "${TARGETARCH}" = "amd64" ]; then \
      ARCH="x86_64"; \
    elif [ "${TARGETARCH}" = "arm64" ]; then \
      ARCH="aarch64"; \
    else \
      echo "Unsupported architecture: ${TARGETARCH}" && exit 1; \
    fi && \
    curl -fsSL "https://github.com/homebridge/ffmpeg-for-homebridge/releases/latest/download/ffmpeg-alpine-${ARCH}.tar.gz" \
      -o /tmp/ffmpeg-homebridge.tar.gz && \
    mkdir -p /tmp/ffmpeg-homebridge && \
    tar -xzf /tmp/ffmpeg-homebridge.tar.gz -C /tmp/ffmpeg-homebridge && \
    cp /tmp/ffmpeg-homebridge/usr/local/bin/ffmpeg /usr/local/bin/ffmpeg-homebridge && \
    chmod +x /usr/local/bin/ffmpeg-homebridge


# 3. Final image
FROM alpine:3.21

RUN apk add --no-cache tini ffmpeg bash curl

# Hardware Acceleration for Intel CPU
ARG TARGETARCH
RUN if [ "${TARGETARCH}" = "amd64" ]; then apk add --no-cache libva-intel-driver intel-media-driver; fi

COPY --from=build /build/go2rtc /usr/local/bin/
COPY --from=ffmpeg-homebridge /usr/local/bin/ffmpeg-homebridge /usr/local/bin/

EXPOSE 1984 8554 8555 8555/udp
ENTRYPOINT ["/sbin/tini", "--"]
VOLUME /config
WORKDIR /config

CMD ["go2rtc", "-config", "/config/go2rtc.yaml"]
