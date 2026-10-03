# 多阶段构建: 区域再生资源回收服务
FROM golang:1.19-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/recycle-app ./cmd/app

FROM alpine:3.19
RUN apk add --no-cache ca-certificates bash
WORKDIR /app
COPY --from=build /out/recycle-app /app/recycle-app
COPY migrations /app/migrations
COPY scripts/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh
EXPOSE 8080
ENTRYPOINT ["/app/docker-entrypoint.sh"]
