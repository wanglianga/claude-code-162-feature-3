# 区域再生资源回收服务 - 多阶段构建
FROM golang:1.19-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/recycle-app ./cmd/app

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates wget \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/recycle-app /app/recycle-app
# 运行时自动迁移需要 migrations 目录
COPY --from=build /src/migrations /app/migrations
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=10 \
  CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/app/recycle-app"]
CMD ["all"]
