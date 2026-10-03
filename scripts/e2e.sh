#!/usr/bin/env bash
# 端到端验证: 完整链路 + 全部异常场景
# 前置: scripts/infra.sh 已执行; 本脚本会重建数据库并启动 server+worker
set -uo pipefail

cd "$(dirname "$0")/.."
export PG_DSN="${PG_DSN:-postgres://postgres@127.0.0.1:5433/recycle?sslmode=disable}"
export REDIS_ADDR="${REDIS_ADDR:-127.0.0.1:6379}"
export KAFKA_BROKERS="${KAFKA_BROKERS:-127.0.0.1:9092}"
export HTTP_ADDR=":18080"
export LATE_GRACE_MINUTES=0
# 积分异常核对参数(缩短时间窗以便端到端验证)
export POINTS_REISSUE_GRACE_SEC=2     # 结算提交 2s 未入账视为系统延迟, 自动补发
export UNSUBMITTED_GRACE_SEC=2        # 居民确认 2s 未提交结算, 生成回收员待处理任务
export CALLBACK_FAIL_WINDOW_SEC=30    # 回调失败统计窗口
export CALLBACK_FAIL_THRESHOLD=3      # 窗口内失败 3 次生成技术排查记录
export CALLBACK_RECOVER_SEC=3         # 3s 无新失败视为回调恢复
BASE="http://127.0.0.1:18080/api/v1"

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  PASS  $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL  $1"; }
check(){ # check <desc> <actual> <expected>
  if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (got=$2 want=$3)"; fi
}
jqget(){ echo "$2" | jq -r "$1"; }

R1="Authorization: Bearer token-resident1"
R2="Authorization: Bearer token-resident2"
C1="Authorization: Bearer token-collector1"
S1="Authorization: Bearer token-station1"
ST="Authorization: Bearer token-street1"
CT="Content-Type: application/json"

# wait_status <order_id> <token-header> <expected> [timeout]
wait_status(){
  local oid="$1" hdr="$2" want="$3" tmo="${4:-15}" i
  for i in $(seq 1 "$tmo"); do
    local st
    st=$(curl -s "$BASE/orders/$oid" -H "$hdr" | jq -r '.order.status')
    [ "$st" = "$want" ] && { echo "$st"; return 0; }
    sleep 1
  done
  curl -s "$BASE/orders/$oid" -H "$hdr" | jq -r '.order.status'
  return 1
}
# wait_balance <expected> [timeout]
wait_balance(){
  local want="$1" tmo="${2:-15}" i
  for i in $(seq 1 "$tmo"); do
    local b
    b=$(curl -s "$BASE/points/balance" -H "$R1" | jq -r '.balance')
    [ "$b" = "$want" ] && { echo "$b"; return 0; }
    sleep 1
  done
  curl -s "$BASE/points/balance" -H "$R1" | jq -r '.balance'
  return 1
}

echo "=== 0. 重建环境 ==="
psql "$PG_DSN" -q -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" 2>/dev/null || { echo "pg reset failed"; exit 1; }
redis-cli -n 0 FLUSHALL >/dev/null
pkill -x recycle-app 2>/dev/null; sleep 1
# 清理 Kafka 历史消息/消费组(本地 KRaft 存储可能残留上次运行的消息), 保证本次运行确定性
KAFKA_DIR="${KAFKA_DIR:-/home/node/infra/kafka}"
if [ -x "$KAFKA_DIR/bin/kafka-topics.sh" ]; then
  for t in recycle.order.events recycle.points.commands recycle.points.events recycle.station.events recycle.notifications; do
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --delete --topic "$t" >/dev/null 2>&1 || true
  done
  for g in points-consumer notification-consumer stats-consumer; do
    "$KAFKA_DIR/bin/kafka-consumer-groups.sh" --bootstrap-server 127.0.0.1:9092 --delete --group "$g" >/dev/null 2>&1 || true
  done
  sleep 3
  for t in recycle.order.events recycle.points.commands recycle.points.events recycle.station.events recycle.notifications; do
    "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server 127.0.0.1:9092 --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1 >/dev/null 2>&1 || true
  done
fi
go build -o /tmp/recycle-app ./cmd/app || { echo "build failed"; exit 1; }
/tmp/recycle-app seed >/dev/null || { echo "seed failed"; exit 1; }
POINTS_CONSUMER_PAUSED=0 /tmp/recycle-app all > /tmp/recycle-app.log 2>&1 &
APP_PID=$!
for i in $(seq 1 20); do curl -sf "http://127.0.0.1:18080/healthz" >/dev/null 2>&1 && break; sleep 1; done
curl -sf "http://127.0.0.1:18080/healthz" >/dev/null || { echo "server not up"; tail -20 /tmp/recycle-app.log; exit 1; }
echo "server up (pid $APP_PID)"

NOW=$(date +%s)
WS=$(date -u -d "@$((NOW+600))" +%Y-%m-%dT%H:%M:%SZ)
WE=$(date -u -d "@$((NOW+3600))" +%Y-%m-%dT%H:%M:%SZ)

echo "=== 1. 居民预约 -> 预估价(价格快照) ==="
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":12.5},{\"category\":\"plastic\",\"est_weight_kg\":5}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",
  \"photos\":[\"p1.jpg\"],\"idempotency_key\":\"e2e-order-1\"}")
OID=$(jqget '.order.id' "$RESP")
EST=$(jqget '.order.estimate_yuan' "$RESP")
VCODE=$(jqget '.order.verify_code' "$RESP")
# 纸12.5kg*1.2=15元 + 塑5kg*2.0=10元 = 25元
check "预估价=25元" "$EST" "25"
[ -n "$OID" ] && [ "$OID" != "null" ] && ok "订单创建 id=$OID" || bad "订单创建 $RESP"

echo "=== 2. 幂等: 同键重复提交 ==="
RESP2=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":12.5},{\"category\":\"plastic\",\"est_weight_kg\":5}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-1\"}")
check "幂等返回同一订单" "$(jqget '.order.id' "$RESP2")" "$OID"

echo "=== 3. 派单(GEO 就近) ==="
check "状态=ASSIGNED" "$(wait_status "$OID" "$R1" ASSIGNED)" "ASSIGNED"

echo "=== 4. 回收员接单->出发->到达 ==="
for act in accept start arrive; do
  curl -s -X POST "$BASE/collector/orders/$OID/$act" -H "$C1" >/dev/null
done
ST1=$(curl -s "$BASE/orders/$OID" -H "$C1" | jq -r '.order.status')
check "状态=ARRIVED" "$ST1" "ARRIVED"

echo "=== 5. 核验居民(核验码) ==="
ERR=$(curl -s -X POST "$BASE/collector/orders/$OID/verify-resident" -H "$C1" -H "$CT" \
  -d '{"method":"SCAN_CODE","verify_code":"000000"}' | jq -r '.error // "none"')
check "错误核验码被拒" "$ERR" "verify_code mismatch"
curl -s -X POST "$BASE/collector/orders/$OID/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$VCODE\"}" >/dev/null
ST1=$(curl -s "$BASE/orders/$OID" -H "$C1" | jq -r '.order.status')
check "状态=RESIDENT_VERIFIED" "$ST1" "RESIDENT_VERIFIED"

echo "=== 6. 居民临时加品类 ==="
ADD=$(curl -s -X POST "$BASE/collector/orders/$OID/items" -H "$C1" -H "$CT" \
  -d '{"category":"metal","est_weight_kg":3}')
check "加品类来源" "$(jqget '.item.source' "$ADD")" "ONSITE_ADDED"

echo "=== 7. 现场称重(含扣杂: 塑料混入10%不可回收物) ==="
W=$(curl -s -X POST "$BASE/collector/orders/$OID/weigh" -H "$C1" -H "$CT" -d '{
  "weights":[{"category":"paper","weight_kg":12.0},
             {"category":"plastic","weight_kg":5.0,"contamination_pm":100},
             {"category":"metal","weight_kg":3.0}],
  "photos":["w1.jpg"]}')
# 纸12*1.2=14.4 + 塑5*0.9*2=9 + 金属3*3.5=10.5 = 33.9元
check "现场定价=33.9元" "$(jqget '.order.onsite_yuan' "$W")" "33.9"
check "状态=WEIGHED" "$(jqget '.order.status' "$W")" "WEIGHED"

echo "=== 8. 居民确认称重 ==="
curl -s -X POST "$BASE/orders/$OID/confirm" -H "$R1" >/dev/null
ST1=$(curl -s "$BASE/orders/$OID" -H "$R1" | jq -r '.order.status')
check "状态=CONFIRMED" "$ST1" "CONFIRMED"

echo "=== 9. 积分结算(Kafka 异步入账) ==="
SE=$(curl -s -X POST "$BASE/collector/orders/$OID/settle" -H "$C1" -H "$CT" -d '{"type":"points"}')
# 33.9元 * 10 * 1.1 = 372.9 -> 373 积分
check "应得积分=373" "$(jqget '.settlement.points' "$SE")" "373"
check "积分到账 balance=373" "$(wait_balance 373)" "373"
check "状态=SETTLED" "$(wait_status "$OID" "$R1" SETTLED)" "SETTLED"

echo "=== 10. 送达站点 -> 复称(容差内) -> 环保凭证 ==="
curl -s -X POST "$BASE/collector/orders/$OID/deliver" -H "$C1" >/dev/null
RW=$(curl -s -X POST "$BASE/station/inbound/$OID/reweigh" -H "$S1" -H "$CT" -d '{
  "weights":[{"category":"paper","weight_kg":11.9},
             {"category":"plastic","weight_kg":5.05},
             {"category":"metal","weight_kg":3.0}]}')
check "状态=COMPLETED" "$(jqget '.order.status' "$RW")" "COMPLETED"
VST=$(jqget '.items[0].variance_status' "$RW")
check "复称差异在容差内" "$VST" "OK"
CERTS=$(curl -s "$BASE/certificates" -H "$R1" | jq -r '.certificates | length')
check "环保凭证=3张" "$CERTS" "3"

echo "=== 11. 全链路追踪 ==="
TR=$(curl -s "$BASE/orders/$OID/trace" -H "$R1")
NCONF=$(echo "$TR" | jq -r '.confirmations | length')
[ "$NCONF" -ge 3 ] && ok "确认记录 $NCONF 条(核验/称重/结算)" || bad "确认记录不足: $NCONF"
NEV=$(echo "$TR" | jq -r '.events | length')
[ "$NEV" -ge 10 ] && ok "状态事件 $NEV 条" || bad "事件不足: $NEV"
check "最终金额存在" "$(echo "$TR" | jq -r '.order.final_cents > 0')" "true"

echo "=== 12. 现金结算 + 支付失败重试 ==="
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"appliance\",\"est_weight_kg\":8}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-2\"}")
OID2=$(jqget '.order.id' "$RESP")
VCODE2=$(jqget '.order.verify_code' "$RESP")
wait_status "$OID2" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID2/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID2/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID2/arrive" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID2/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$VCODE2\"}" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID2/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"appliance","weight_kg":8}]}' >/dev/null
curl -s -X POST "$BASE/orders/$OID2/confirm" -H "$R1" >/dev/null
F1=$(curl -s -X POST "$BASE/collector/orders/$OID2/settle" -H "$C1" -H "$CT" \
  -d '{"type":"cash","simulate_fail":true}')
check "现金支付失败" "$(jqget '.settlement.status' "$F1")" "FAILED"
ST1=$(curl -s "$BASE/orders/$OID2" -H "$R1" | jq -r '.order.status')
check "失败后订单停留 CONFIRMED" "$ST1" "CONFIRMED"
F2=$(curl -s -X POST "$BASE/collector/orders/$OID2/settle/retry" -H "$C1" -H "$CT" -d '{}')
check "重试成功" "$(jqget '.settlement.status' "$F2")" "SUCCESS"
check "重试后 SETTLED" "$(jqget '.order.status' "$F2")" "SETTLED"
# 8kg 家电: 现场 8*1.5=12元
check "现金金额=12元" "$(jqget '.settlement.amount_yuan' "$F2")" "12"

echo "=== 13. 老人协助确认 ==="
RESP=$(curl -s -X POST "$BASE/orders" -H "$R2" -H "$CT" -d "{
  \"items\":[{\"category\":\"clothing\",\"est_weight_kg\":6}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-3\"}")
OID3=$(jqget '.order.id' "$RESP")
wait_status "$OID3" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3/arrive" -H "$C1" >/dev/null
# 老人不会扫码: 回收员协助核验+协助确认
curl -s -X POST "$BASE/collector/orders/$OID3/verify-resident" -H "$C1" -H "$CT" \
  -d '{"method":"ASSISTED","assisted_note":"老人无智能手机, 口头确认并拍照"}' >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"clothing","weight_kg":6}]}' >/dev/null
AC=$(curl -s -X POST "$BASE/collector/orders/$OID3/confirm-assisted" -H "$C1" -H "$CT" \
  -d '{"assisted_note":"老人确认重量6kg, 女儿电话复核"}')
check "协助确认后 CONFIRMED" "$(jqget '.order.status' "$AC")" "CONFIRMED"
# 现金结算并送达完结, 避免滞留 CONFIRMED(否则会被积分核对识别为回收员未提交)
SS3=$(curl -s -X POST "$BASE/collector/orders/$OID3/settle" -H "$C1" -H "$CT" -d '{"type":"cash"}')
check "老人单现金结算 SETTLED" "$(jqget '.order.status' "$SS3")" "SETTLED"
curl -s -X POST "$BASE/collector/orders/$OID3/deliver" -H "$C1" >/dev/null
curl -s -X POST "$BASE/station/inbound/$OID3/reweigh" -H "$S1" -H "$CT" \
  -d '{"weights":[{"category":"clothing","weight_kg":6}]}' >/dev/null
M=$(curl -s "$BASE/orders/$OID3/trace" -H "$ST" | jq -r '[.confirmations[].method] | join(",")')
echo "$M" | grep -q "ASSISTED" && ok "协助确认留痕 method=ASSISTED" || bad "协助确认留痕: $M"
# 非老人用协助确认应被拒
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":2}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-3b\"}")
OID3B=$(jqget '.order.id' "$RESP")
wait_status "$OID3B" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3B/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3B/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID3B/arrive" -H "$C1" >/dev/null
ERR=$(curl -s -X POST "$BASE/collector/orders/$OID3B/verify-resident" -H "$C1" -H "$CT" \
  -d '{"method":"ASSISTED","assisted_note":"x"}' | jq -r '.error // "none"')
check "非老人协助确认被拒" "$ERR" "assisted confirm only allowed for elderly residents"
curl -s -X POST "$BASE/orders/$OID3B/cancel" -H "$R1" -H "$CT" -d '{"reason":"test cleanup"}' >/dev/null

echo "=== 14. 称重争议 -> 街道裁决 ==="
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"metal\",\"est_weight_kg\":10}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-4\"}")
OID4=$(jqget '.order.id' "$RESP")
VCODE4=$(jqget '.order.verify_code' "$RESP")
wait_status "$OID4" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID4/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID4/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID4/arrive" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID4/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$VCODE4\"}" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID4/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"metal","weight_kg":8}]}' >/dev/null
D=$(curl -s -X POST "$BASE/orders/$OID4/disputes" -H "$R1" -H "$CT" \
  -d '{"kind":"WEIGHT","detail":"居民复称约10kg, 与回收员8kg不一致"}')
DID=$(jqget '.dispute.id' "$D")
ST1=$(curl -s "$BASE/orders/$OID4" -H "$R1" | jq -r '.order.status')
check "争议后 DISPUTED" "$ST1" "DISPUTED"
curl -s -X POST "$BASE/street/disputes/$DID/resolve" -H "$ST" -H "$CT" \
  -d '{"action":"adjust","note":"站点视频复核为9.5kg","adjusted_weights":[{"category":"metal","weight_kg":9.5}]}' >/dev/null
ST1=$(curl -s "$BASE/orders/$OID4" -H "$R1" | jq -r '.order.status')
check "裁决后回到 WEIGHED" "$ST1" "WEIGHED"
curl -s -X POST "$BASE/orders/$OID4/confirm" -H "$R1" >/dev/null
SE=$(curl -s -X POST "$BASE/collector/orders/$OID4/settle" -H "$C1" -H "$CT" -d '{"type":"cash"}')
# 9.5kg * 3.5 = 33.25元
check "裁决后按9.5kg结算=33.25元" "$(jqget '.settlement.amount_yuan' "$SE")" "33.25"

echo "=== 15. 复称差异超差 -> 收益扣罚 ==="
curl -s -X POST "$BASE/collector/orders/$OID4/deliver" -H "$C1" >/dev/null
# 现场 9.5kg, 站点复称 8kg: 差 1.5kg (15.8%) 超差
RW=$(curl -s -X POST "$BASE/station/inbound/$OID4/reweigh" -H "$S1" -H "$CT" \
  -d '{"weights":[{"category":"metal","weight_kg":8}]}')
check "超差标记" "$(jqget '.items[0].variance_status' "$RW")" "OVER_TOLERANCE"
EARN=$(curl -s "$BASE/collector/earnings" -H "$C1")
PEN=$(echo "$EARN" | jq -r ".earnings[] | select(.order_id==$OID4) | .penalty_yuan")
# 差异 9.5*3.5-8*3.5=5.25元 扣罚
check "回收员扣罚=5.25元" "$PEN" "5.25"

echo "=== 16. 积分不到账 -> 申诉对账 ==="
kill "$APP_PID" 2>/dev/null; sleep 1
POINTS_CONSUMER_PAUSED=1 /tmp/recycle-app all > /tmp/recycle-app2.log 2>&1 &
APP_PID=$!
for i in $(seq 1 20); do curl -sf "http://127.0.0.1:18080/healthz" >/dev/null 2>&1 && break; sleep 1; done
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":10}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-5\"}")
OID5=$(jqget '.order.id' "$RESP")
VCODE5=$(jqget '.order.verify_code' "$RESP")
wait_status "$OID5" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID5/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID5/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID5/arrive" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID5/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$VCODE5\"}" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID5/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"paper","weight_kg":10}]}' >/dev/null
curl -s -X POST "$BASE/orders/$OID5/confirm" -H "$R1" >/dev/null
BAL_BEFORE=$(curl -s "$BASE/points/balance" -H "$R1" | jq -r '.balance')
curl -s -X POST "$BASE/collector/orders/$OID5/settle" -H "$C1" -H "$CT" -d '{"type":"points"}' >/dev/null
sleep 3
BAL_AFTER=$(curl -s "$BASE/points/balance" -H "$R1" | jq -r '.balance')
check "消费者暂停: 积分未到账" "$BAL_AFTER" "$BAL_BEFORE"
CL=$(curl -s -X POST "$BASE/points/claims" -H "$R1" -H "$CT" -d "{\"order_id\":$OID5,\"note\":\"积分没到账\"}")
check "申诉后补账" "$(jqget '.status' "$CL")" "RESOLVED_CREDITED"
# 10kg纸=12元 -> 132 积分
check "补账积分=132" "$(jqget '.credited' "$CL")" "132"
CL2=$(curl -s -X POST "$BASE/points/claims" -H "$R1" -H "$CT" -d "{\"order_id\":$OID5}")
check "重复申诉=已入账" "$(jqget '.status' "$CL2")" "ALREADY_CREDITED"
ST1=$(curl -s "$BASE/orders/$OID5" -H "$R1" | jq -r '.order.status')
check "补账后 SETTLED" "$ST1" "SETTLED"

echo "=== 17. 回收员迟到 ==="
PAST_WS=$(date -u -d "@$((NOW-1800))" +%Y-%m-%dT%H:%M:%SZ)
PAST_WE=$(date -u -d "@$((NOW-60))" +%Y-%m-%dT%H:%M:%SZ)
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":1}],
  \"time_window_start\":\"$PAST_WS\",\"time_window_end\":\"$PAST_WE\",\"idempotency_key\":\"e2e-order-6\"}")
OID6=$(jqget '.order.id' "$RESP")
LATE=0
for i in $(seq 1 20); do
  LATE=$(curl -s "$BASE/orders/$OID6" -H "$R1" | jq -r '.order.late_minutes')
  [ "$LATE" -gt 0 ] && break
  sleep 1
done
[ "$LATE" -gt 0 ] && ok "迟到标记 late_minutes=$LATE" || bad "迟到未标记"
NOTIF=$(curl -s "$BASE/notifications" -H "$R1" | jq -r '[.notifications[] | select(.kind=="ORDER_LATE")] | length')
[ "$NOTIF" -ge 1 ] && ok "迟到通知已送达" || bad "迟到通知缺失"
CC=$(curl -s -X POST "$BASE/orders/$OID6/cancel" -H "$R1" -H "$CT" -d '{"reason":"回收员迟到, 改约"}')
check "迟到后取消" "$(jqget '.order.status' "$CC")" "CANCELLED"

echo "=== 18. 投诉 ==="
CMP=$(curl -s -X POST "$BASE/orders/$OID/complaints" -H "$R1" -H "$CT" \
  -d '{"content":"回收员称重时未去皮"}')
CMPID=$(jqget '.complaint.id' "$CMP")
curl -s -X POST "$BASE/street/complaints/$CMPID/resolve" -H "$ST" -H "$CT" \
  -d '{"resolution":"已约谈回收员, 复称无误差, 向居民解释"}' >/dev/null
CST=$(curl -s "$BASE/street/complaints?status=RESOLVED" -H "$ST" | jq -r "[.complaints[] | select(.id==$CMPID)] | length")
check "投诉已处理" "$CST" "1"

echo "=== 19. 街道视角: 参与与资源流向 ==="
STATS=$(curl -s "$BASE/street/stats?street=城东街道" -H "$ST")
[ "$(jqget '.participating_residents' "$STATS")" -ge 2 ] && ok "参与居民>=2" || bad "参与居民统计异常"
FLOWS=$(curl -s "$BASE/street/flows?street=城东街道" -H "$ST")
FLOWN=$(jqget '.flows | length' "$FLOWS")
[ "$FLOWN" -ge 3 ] && ok "资源流向 $FLOWN 条(按品类)" || bad "资源流向不足: $FLOWN"

echo "=== 20. 站点入库质量 ==="
Q=$(curl -s "$BASE/station/quality" -H "$S1")
QN=$(jqget '.quality | length' "$Q")
[ "$QN" -ge 3 ] && ok "入库质量统计 $QN 个品类" || bad "入库质量统计不足"

echo "=== 21. 市场价更新(版本化) ==="
curl -s -X POST "$BASE/street/prices" -H "$ST" -H "$CT" \
  -d '{"category":"paper","price_yuan_per_kg":1.5}' >/dev/null
P=$(curl -s "$BASE/prices" -H "$R1" | jq -r '.prices[] | select(.category=="paper") | .price_yuan_per_kg')
check "新价=1.5元" "$P" "1.5"
# 老订单价格快照不受影响: 订单1的纸单价仍是1.2
OLDP=$(curl -s "$BASE/orders/$OID" -H "$R1" | jq -r '.items[] | select(.category=="paper") | .est_price_yuan')
check "老订单价格快照=1.2元" "$OLDP" "1.2"

# 复用辅助: 完成一单到 CONFIRMED(回收员 C1, 居民 token 由 $1 传入, 幂等键 $2, 品类/重量 $3/$4)
# 输出订单号到 LAST_OID
drive_to_confirmed(){
  local hdr="$1" key="$2" cat="$3" wkg="$4"
  local resp vcode
  resp=$(curl -s -X POST "$BASE/orders" -H "$hdr" -H "$CT" -d "{
    \"items\":[{\"category\":\"$cat\",\"est_weight_kg\":$wkg}],
    \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"$key\"}")
  LAST_OID=$(jqget '.order.id' "$resp")
  vcode=$(jqget '.order.verify_code' "$resp")
  wait_status "$LAST_OID" "$C1" ASSIGNED >/dev/null
  curl -s -X POST "$BASE/collector/orders/$LAST_OID/accept" -H "$C1" >/dev/null
  curl -s -X POST "$BASE/collector/orders/$LAST_OID/start" -H "$C1" >/dev/null
  curl -s -X POST "$BASE/collector/orders/$LAST_OID/arrive" -H "$C1" >/dev/null
  curl -s -X POST "$BASE/collector/orders/$LAST_OID/verify-resident" -H "$C1" -H "$CT" \
    -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$vcode\"}" >/dev/null
  curl -s -X POST "$BASE/collector/orders/$LAST_OID/weigh" -H "$C1" -H "$CT" \
    -d "{\"weights\":[{\"category\":\"$cat\",\"weight_kg\":$wkg}]}" >/dev/null
  curl -s -X POST "$BASE/orders/$LAST_OID/confirm" -H "$hdr" >/dev/null
}

echo "=== 22. 老人旧衣回收积分延迟 -> 定时核对自动补发并通知 ==="
drive_to_confirmed "$R2" "e2e-order-7" "clothing" 5
OID7="$LAST_OID"; ORDERNO7=$(curl -s "$BASE/orders/$OID7" -H "$R2" | jq -r '.order.order_no')
BAL7_BEFORE=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
curl -s -X POST "$BASE/collector/orders/$OID7/settle" -H "$C1" -H "$CT" -d '{"type":"points"}' >/dev/null
# 等待定时核对(5s tick + 2s 宽限)自动补发: 旧衣 5kg*0.8元=4元 -> 4*10*1.1=44 积分
ST7=""
for i in $(seq 1 20); do
  ST7=$(curl -s "$BASE/orders/$OID7" -H "$R2" | jq -r '.order.status')
  [ "$ST7" = "SETTLED" ] && break
  sleep 1
done
check "延迟后自动补发推进 SETTLED" "$ST7" "SETTLED"
BAL7_AFTER=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
check "老人旧衣积分自动补发(+44)" "$((BAL7_AFTER-BAL7_BEFORE))" "44"
RN=$(curl -s "$BASE/notifications" -H "$R2" | jq -r '[.notifications[] | select(.kind=="POINTS_REISSUED")] | length')
[ "$RN" -ge 1 ] && ok "补发通知已送达老人" || bad "补发通知缺失"
RI=$(curl -s "$BASE/points/reissues" -H "$R2" | jq -r ".reissues[] | select(.original_order_id==$OID7)")
check "补发记录-原订单编号" "$(echo "$RI" | jq -r '.original_order_no')" "$ORDERNO7"
echo "$RI" | jq -e '.credited_at != null and .credited_at != ""' >/dev/null && ok "补发记录-到账时间存在" || bad "补发记录-到账时间缺失"
RSRC=$(echo "$RI" | jq -r '.source')
echo "$RSRC" | grep -q "系统自动对账补发" && ok "补发记录-来源: $RSRC" || bad "补发来源异常: $RSRC"
# 余额流水中补发流水带三要素
ENR=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r ".entries[] | select(.order_id==$OID7 and .reissue_source != null) | .original_order_no")
check "余额流水展示原订单号" "$ENR" "$ORDERNO7"
# 运营四要素核对
CK=$(curl -s "$BASE/street/orders/$OID7/points-check" -H "$ST")
check "核对-居民账号" "$(jqget '.check.account_verified' "$CK")" "true"
check "核对-回收员已提交" "$(jqget '.check.submitted' "$CK")" "true"
check "核对-回调已入账" "$(jqget '.check.callback_credited' "$CK")" "true"
check "核对-规则应得积分=44" "$(jqget '.check.expected_points' "$CK")" "44"
echo "$CK" | jq -e '.check.diagnosis == "OK_CREDITED" or .check.diagnosis == "REISSUED"' >/dev/null \
  && ok "核对诊断: $(jqget '.check.diagnosis' "$CK")" || bad "核对诊断异常: $(jqget '.check.diagnosis' "$CK")"

echo "=== 23. 回收员未提交 -> 待处理任务影响结算, 补提交后自动关闭 ==="
drive_to_confirmed "$R1" "e2e-order-8" "plastic" 3
OID8="$LAST_OID"
# 未结算期间无收益记录
EARN8=$(curl -s "$BASE/collector/earnings" -H "$C1" | jq -r "[.earnings[] | select(.order_id==$OID8)] | length")
check "未提交: 收益暂不生成" "$EARN8" "0"
TASKN=0
for i in $(seq 1 20); do
  TASKN=$(curl -s "$BASE/collector/tasks?status=OPEN" -H "$C1" | jq -r "[.tasks[] | select(.order_id==$OID8)] | length")
  [ "$TASKN" = "1" ] && break
  sleep 1
done
check "生成回收员待处理任务" "$TASKN" "1"
TASK=$(curl -s "$BASE/collector/tasks?status=OPEN" -H "$C1" | jq -r ".tasks[] | select(.order_id==$OID8)")
echo "$TASK" | jq -e '.impact | contains("结算挂起")' >/dev/null && ok "任务说明影响结算: $(echo "$TASK" | jq -r '.kind')" || bad "任务影响说明缺失"
PN=$(curl -s "$BASE/notifications" -H "$R1" | jq -r '[.notifications[] | select(.kind=="POINTS_PENDING_SUBMISSION")] | length')
[ "$PN" -ge 1 ] && ok "居民收到未提交提示通知" || bad "居民未提交提示缺失"
# 回收员补提交(现金): 任务自动关闭, 收益生成
SR=$(curl -s -X POST "$BASE/collector/orders/$OID8/settle" -H "$C1" -H "$CT" -d '{"type":"cash"}')
check "补提交后 SETTLED" "$(jqget '.order.status' "$SR")" "SETTLED"
OPEN8=$(curl -s "$BASE/collector/tasks?status=OPEN" -H "$C1" | jq -r "[.tasks[] | select(.order_id==$OID8)] | length")
check "本订单未结任务已关闭" "$OPEN8" "0"
RESN=$(curl -s "$BASE/collector/tasks?status=RESOLVED" -H "$C1" | jq -r "[.tasks[] | select(.order_id==$OID8)] | length")
check "任务已自动关闭" "$RESN" "1"
EARN8=$(curl -s "$BASE/collector/earnings" -H "$C1" | jq -r "[.earnings[] | select(.order_id==$OID8)] | length")
check "补提交后收益恢复生成" "$EARN8" "1"

echo "=== 24. 消息回调频繁失败 -> 技术排查记录 + 受影响居民名单 ==="
drive_to_confirmed "$R1" "e2e-order-9" "metal" 2
OID9="$LAST_OID"; ORDERNO9=$(curl -s "$BASE/orders/$OID9" -H "$R1" | jq -r '.order.order_no')
curl -s -X POST "$BASE/collector/orders/$OID9/settle" -H "$C1" -H "$CT" -d '{"type":"points"}' >/dev/null
# 暂停的消费者约 2.5s 重试失败一次; 注: 第16场景申诉补账的消息仍会重试, 排查记录可能已打开
INC_ST="NONE"
for i in $(seq 1 24); do
  INC_ST=$(curl -s "$BASE/street/tech-incidents?status=OPEN" -H "$ST" | jq -r '.incidents | length')
  [ "$INC_ST" -ge 1 ] && break
  sleep 1
done
[ "$INC_ST" -ge 1 ] && ok "技术排查记录已生成(OPEN)" || bad "技术排查记录缺失"
INC=$(curl -s "$BASE/street/tech-incidents?status=OPEN" -H "$ST")
FC=$(jqget '.incidents[0].fail_count' "$INC")
[ "$FC" -ge 3 ] && ok "失败计数=$FC(>=阈值3)" || bad "失败计数异常: $FC"
INOTIF=0
for i in $(seq 1 12); do
  INOTIF=$(curl -s "$BASE/notifications" -H "$ST" | jq -r '[.notifications[] | select(.kind=="TECH_INCIDENT_OPENED")] | length')
  [ "$INOTIF" -ge 1 ] && break
  sleep 1
done
[ "$INOTIF" -ge 1 ] && ok "运营收到排查通知" || bad "运营排查通知缺失"
# 受影响居民名单(账号/电话/订单号/失败次数, 便于集中解释)
# 注: 22 场景老人旧衣单补发前也会失败, 名单可能含多位居民; 精确定位本次 resident1 行
AFF=$(curl -s "$BASE/street/callback-affected?since_minutes=60" -H "$ST")
ACNT=$(jqget '.affected_count' "$AFF")
[ "$ACNT" -ge 1 ] && ok "名单含受影响居民($ACNT 位)" || bad "名单为空"
R1ROW=$(echo "$AFF" | jq -c '.residents[] | select(.account=="resident1")')
[ -n "$R1ROW" ] && ok "名单含本次居民 resident1" || bad "名单缺少 resident1"
check "名单居民账号" "$(echo "$R1ROW" | jq -r '.account')" "resident1"
echo "$R1ROW" | jq -e '.phone | length > 0' >/dev/null && ok "名单含联系电话" || bad "名单电话缺失"
# 等待本订单的失败也计入名单(同一居民可能含多笔订单), 校验包含本单原订单号
ORDERS_IN_LIST=""
for i in $(seq 1 12); do
  AFF=$(curl -s "$BASE/street/callback-affected?since_minutes=60" -H "$ST")
  R1ROW=$(echo "$AFF" | jq -c '.residents[] | select(.account=="resident1")')
  ORDERS_IN_LIST=$(echo "$R1ROW" | jq -r '.order_nos')
  echo "$ORDERS_IN_LIST" | grep -q "$ORDERNO9" && break
  sleep 1
done
echo "$ORDERS_IN_LIST" | grep -q "$ORDERNO9" && ok "名单含本单原订单号 $ORDERNO9" || bad "名单原订单号缺失: $ORDERS_IN_LIST"
AFC=$(echo "$R1ROW" | jq -r '.fail_count')
[ "$AFC" -ge 1 ] && ok "名单失败次数=$AFC(阈值按通道累计)" || bad "名单失败次数异常: $AFC"
# 恢复消费者: 重启为正常进程, 挂账被自动补发, 排查记录自动关闭
kill "$APP_PID" 2>/dev/null; sleep 1
POINTS_CONSUMER_PAUSED=0 /tmp/recycle-app all > /tmp/recycle-app3.log 2>&1 &
APP_PID=$!
for i in $(seq 1 20); do curl -sf "http://127.0.0.1:18080/healthz" >/dev/null 2>&1 && break; sleep 1; done
RESOLVED=0
for i in $(seq 1 24); do
  RESOLVED=$(curl -s "$BASE/street/tech-incidents?status=RESOLVED" -H "$ST" | jq -r '.incidents | length')
  [ "$RESOLVED" -ge 1 ] && break
  sleep 1
done
[ "$RESOLVED" -ge 1 ] && ok "回调恢复后排查记录自动关闭" || bad "排查记录未关闭"
# 挂账已补发(消费者恢复正常入账或定时核对自动补发), 轮询等待
ST9=$(wait_status "$OID9" "$R1" SETTLED 20 || true)
check "恢复后挂账补发 SETTLED" "$ST9" "SETTLED"
# 恢复通知经 outbox 异步投递, 轮询等待落库
RN2=0
for i in $(seq 1 12); do
  RN2=$(curl -s "$BASE/notifications" -H "$ST" | jq -r '[.notifications[] | select(.kind=="TECH_INCIDENT_RESOLVED")] | length')
  [ "$RN2" -ge 1 ] && break
  sleep 1
done
[ "$RN2" -ge 1 ] && ok "运营收到恢复通知" || bad "运营恢复通知缺失"
# 名单标注已补发数量(等补发记录计入, 精确定位 resident1 行)
ARC=0
for i in $(seq 1 10); do
  ARC=$(curl -s "$BASE/street/callback-affected?since_minutes=60" -H "$ST" \
    | jq -r '[.residents[] | select(.account=="resident1")][0].reissued_count // 0')
  [ "$ARC" -ge 1 ] && break
  sleep 1
done
[ "$ARC" -ge 1 ] && ok "名单标注已补发=$ARC" || bad "名单补发数量异常: $ARC"

echo "=== 25. 运营手动触发核对 + 权限校验 ==="
# 居民无权触发
RC_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$BASE/street/points/reconcile" -H "$R1")
check "居民触发核对被拒(403)" "$RC_CODE" "403"
# 街道可触发; 此时无滞留 PENDING/未提交, 返回 0 补发 0 任务
RC=$(curl -s -X POST "$BASE/street/points/reconcile" -H "$ST")
check "手动核对返回补发列表" "$(echo "$RC" | jq -r '.reissued | type')" "array"
echo "$RC" | jq -e '.tasks_created == 0' >/dev/null && ok "手动核对执行成功(无新增任务)" || bad "手动核对异常: $RC"
# 技术排查记录列表可查(含已关闭)
ICN=$(curl -s "$BASE/street/tech-incidents" -H "$ST" | jq -r '.incidents | length')
[ "$ICN" -ge 1 ] && ok "技术排查记录可查 $ICN 条" || bad "技术排查记录查询异常"

echo "=== 26. 居民端补发记录三要素汇总 ==="
# resident1: 第16场景申诉补账; resident2(老人): 第22场景旧衣单系统延迟自动补发
N1=$(curl -s "$BASE/points/reissues" -H "$R1" | jq -r '.reissues | length')
N2=$(curl -s "$BASE/points/reissues" -H "$R2" | jq -r '.reissues | length')
[ "$N1" -ge 1 ] && ok "居民1 申诉补发记录 $N1 条" || bad "居民1 补发记录异常: $N1"
[ "$N2" -ge 1 ] && ok "老人 自动补发记录 $N2 条" || bad "老人 补发记录异常: $N2"
BAD=0
for hdr in "$R1" "$R2"; do
  B=$(curl -s "$BASE/points/reissues" -H "$hdr" \
    | jq -r '[.reissues[] | select(.source=="" or .credited_at==null or .original_order_no=="")] | length')
  BAD=$((BAD+B))
done
check "每条补发含来源/到账时间/原订单编号" "$BAD" "0"

echo "=== 27. OpenAPI 3 文档与 Swagger UI ==="
DOCHTTP=$(curl -s -o /tmp/docs-page.html -w "%{http_code}" "http://127.0.0.1:18080/docs/")
check "Swagger UI 可访问" "$DOCHTTP" "200"
grep -q "SwaggerUIBundle" /tmp/docs-page.html && ok "Swagger UI 页面加载" || bad "Swagger UI 内容缺失"
SPECOK=$(curl -s "http://127.0.0.1:18080/openapi.json" | jq -r 'select(.openapi|startswith("3.")) | "yes"')
check "OpenAPI 3.x 规范" "${SPECOK:-no}" "yes"
HASNEW=$(curl -s "http://127.0.0.1:18080/openapi.json" | jq -r '[.paths | keys[] | select(test("points/reconcile|callback-affected|points-check|tech-incidents|collector/tasks|points/reissues"))] | length')
[ "$HASNEW" -ge 6 ] && ok "规范包含积分异常相关接口($HASNEW)" || bad "规范缺少新接口: $HASNEW"

echo
echo "==================================="
echo "PASS=$PASS FAIL=$FAIL"
kill "$APP_PID" 2>/dev/null
[ "$FAIL" -eq 0 ] && echo "E2E_ALL_PASS" || echo "E2E_HAS_FAILURES"
exit $FAIL
