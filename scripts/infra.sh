#!/usr/bin/env bash
# 基础设施一键准备: PostgreSQL(5433) + Redis(6379, 系统已有) + Kafka KRaft(9092)
# 幂等: 已运行则跳过
set -euo pipefail

INFRA_DIR="${INFRA_DIR:-/home/node/infra}"
PGDATA="${PGDATA:-/home/node/pgdata}"
PGPORT="${PGPORT:-5433}"
KAFKA_DIR="$INFRA_DIR/kafka"
KAFKA_PORT="${KAFKA_PORT:-9092}"
export PATH=/usr/lib/postgresql/15/bin:$PATH

echo "==> PostgreSQL @ $PGPORT"
if pg_ctl -D "$PGDATA" status >/dev/null 2>&1; then
  echo "    already running"
else
  if [ ! -d "$PGDATA" ]; then
    initdb -D "$PGDATA" -U postgres --auth-local=trust --auth-host=trust -E UTF8 --locale=C.UTF-8 >/dev/null
    cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '/tmp'
listen_addresses = '127.0.0.1'
EOF
  fi
  mkdir -p /home/node/pglog
  pg_ctl -D "$PGDATA" -l /home/node/pglog/server.log -w start >/dev/null
  echo "    started"
fi
psql -h 127.0.0.1 -p "$PGPORT" -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='recycle'" | grep -q 1 \
  || psql -h 127.0.0.1 -p "$PGPORT" -U postgres -c "CREATE DATABASE recycle" >/dev/null
echo "    database recycle ready"

echo "==> Redis @ 6379"
redis-cli ping >/dev/null && echo "    ok"

echo "==> Kafka @ $KAFKA_PORT"
if (exec 3<>/dev/tcp/127.0.0.1/$KAFKA_PORT) 2>/dev/null; then
  echo "    already running"
else
  if [ ! -d "$KAFKA_DIR" ]; then
    mkdir -p "$INFRA_DIR"
    TARBALL="$INFRA_DIR/kafka.tgz"
    if [ ! -s "$TARBALL" ]; then
      echo "    downloading kafka..."
      curl -sL --max-time 600 -o "$TARBALL" \
        https://mirrors.huaweicloud.com/apache/kafka/3.9.1/kafka_2.13-3.9.1.tgz
    fi
    mkdir -p "$KAFKA_DIR"
    tar xzf "$TARBALL" -C "$KAFKA_DIR" --strip-components=1
  fi
  export KAFKA_HEAP_OPTS="-Xmx512M -Xms256M"
  cd "$KAFKA_DIR"
  if [ ! -d /tmp/kraft-combined-logs ]; then
    UUID=$(bin/kafka-storage.sh random-uuid)
    bin/kafka-storage.sh format -t "$UUID" -c config/kraft/server.properties >/dev/null
  fi
  nohup bin/kafka-server-start.sh config/kraft/server.properties > "$INFRA_DIR/kafka.log" 2>&1 &
  echo "    starting (pid $!)..."
  for i in $(seq 1 60); do
    if (exec 3<>/dev/tcp/127.0.0.1/$KAFKA_PORT) 2>/dev/null; then break; fi
    sleep 2
  done
fi
# 建 topic(幂等)
cd "$KAFKA_DIR" 2>/dev/null && for t in recycle.order.events recycle.points.commands recycle.points.events recycle.station.events recycle.notifications; do
  bin/kafka-topics.sh --bootstrap-server 127.0.0.1:$KAFKA_PORT --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
done
echo "    topics ready"
echo "ALL INFRA OK"
