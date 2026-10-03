#!/usr/bin/env bash
# 容器入口: 等待依赖 -> 迁移(应用启动时自动执行) -> 幂等种子 -> 全进程运行
set -euo pipefail

wait_tcp(){ # wait_tcp host port name
  local host="$1" port="$2" name="$3" i
  echo "==> waiting for $name $host:$port"
  for ((i=0;i<120;i++)); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then return 0; fi
    sleep 1
  done
  echo "$name not reachable"; return 1
}

wait_tcp "${PG_HOST:-postgres}" "${PG_PORT:-5432}" "postgres"
wait_tcp "${REDIS_HOST:-redis}" "${REDIS_PORT:-6379}" "redis"
wait_tcp "${KAFKA_HOST:-kafka}" "${KAFKA_PORT:-9092}" "kafka"
sleep 2

echo "==> seed (idempotent)"
/app/recycle-app seed || true

echo "==> start all (http api + dispatch + reconcile timers + kafka relay/consumers)"
exec /app/recycle-app all
