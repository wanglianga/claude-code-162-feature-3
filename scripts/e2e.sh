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
# 老人单收尾: 现金结算, 订单到 SETTLED(避免遗留 CONFIRMED 被异常扫描判为未提交)
CS13=$(curl -s -X POST "$BASE/collector/orders/$OID3/settle" -H "$C1" -H "$CT" -d '{"type":"cash"}')
check "老人单现金结算成功" "$(jqget '.settlement.status' "$CS13")" "SUCCESS"
check "老人单结算后 SETTLED" "$(jqget '.order.status' "$CS13")" "SETTLED"
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

echo "=== 22. 老人旧衣回收: 系统延迟 -> 定时自动补发(四方核对) ==="
kill "$APP_PID" 2>/dev/null; sleep 1
POINTS_CONSUMER_PAUSED=1 POINTS_DELAY_GRACE_SECONDS=2 MISSING_SETTLE_GRACE_SECONDS=2 \
  /tmp/recycle-app all > /tmp/recycle-app3.log 2>&1 &
APP_PID=$!
for i in $(seq 1 20); do curl -sf "http://127.0.0.1:18080/healthz" >/dev/null 2>&1 && break; sleep 1; done
RESP=$(curl -s -X POST "$BASE/orders" -H "$R2" -H "$CT" -d "{
  \"items\":[{\"category\":\"clothing\",\"est_weight_kg\":6}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-7\"}")
OID7=$(jqget '.order.id' "$RESP")
VCODE7=$(jqget '.order.verify_code' "$RESP")
wait_status "$OID7" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/arrive" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/verify-resident" -H "$C1" -H "$CT" \
  -d '{"method":"ASSISTED","assisted_note":"老人无智能手机, 口头确认"}' >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"clothing","weight_kg":6}]}' >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID7/confirm-assisted" -H "$C1" -H "$CT" \
  -d '{"assisted_note":"老人确认6kg旧衣"}' >/dev/null
SE7=$(curl -s -X POST "$BASE/collector/orders/$OID7/settle" -H "$C1" -H "$CT" -d '{"type":"points"}')
# 6kg 旧衣 * 0.8元 = 4.8元 -> 480*10*1.1 = 52.8 -> 53 积分
check "旧衣应得积分=53" "$(jqget '.settlement.points' "$SE7")" "53"
BAL7_0=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
sleep 3
BAL7_1=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
check "消费者暂停期间积分未到账" "$BAL7_1" "$BAL7_0"
# 定时对账(宽限2s): 系统延迟自动补发
BAL7=999
for i in $(seq 1 25); do
  BAL7=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
  [ "$BAL7" = "53" ] && break; sleep 1
done
check "自动补发后余额=53" "$BAL7" "53"
check "补发后订单 SETTLED" "$(wait_status "$OID7" "$R2" SETTLED 20)" "SETTLED"
# 居民端: 补发来源/到账时间/原订单编号
RI=$(curl -s "$BASE/points/reissues" -H "$R2")
check "补发来源=AUTO_REISSUE" "$(jqget '.reissues[0].source' "$RI")" "AUTO_REISSUE"
check "补发原因=SYSTEM_DELAY" "$(jqget '.reissues[0].reissue_reason' "$RI")" "SYSTEM_DELAY"
ONO7=$(jqget '.order.order_no' "$RESP")
check "补发记录含原订单编号" "$(jqget '.reissues[0].origin_order_no' "$RI")" "$ONO7"
HAS_TS=$(echo "$RI" | jq -r '.reissues[0].credited_at != null and .reissues[0].credited_at != ""')
check "补发记录含到账时间" "$HAS_TS" "true"
# 余额流水同样带来源与原订单号
BE=$(curl -s "$BASE/points/balance" -H "$R2")
check "流水含补发来源" "$(echo "$BE" | jq -r '.entries[0].source')" "AUTO_REISSUE"
check "流水含原订单编号" "$(echo "$BE" | jq -r '.entries[0].origin_order_no')" "$ONO7"
# 补发通知已送达且包含原订单编号(relay+通知消费异步, 等待)
for i in $(seq 1 20); do
  NOTIF7=$(curl -s "$BASE/notifications" -H "$R2" | jq -r '[.notifications[]|select(.kind=="POINTS_REISSUED")]|length')
  [ "$NOTIF7" -ge 1 ] && break; sleep 1
done
[ "$NOTIF7" -ge 1 ] && ok "补发通知已送达" || bad "补发通知缺失"
curl -s "$BASE/notifications" -H "$R2" | jq -r '.notifications[].content' | grep -q "$ONO7" \
  && ok "补发通知含原订单编号" || bad "补发通知未含原订单编号"
# 幂等: 消费者持续暂停, 余额不得重复增加
sleep 4
BAL7_2=$(curl -s "$BASE/points/balance" -H "$R2" | jq -r '.balance')
check "暂停恢复后无重复入账" "$BAL7_2" "53"
# 居民再次发起对账: 已入账 + 四方核对结果
RC7=$(curl -s -X POST "$BASE/points/reconcile" -H "$R2" -H "$CT" -d "{\"order_id\":$OID7}")
check "对账结论=ALREADY_CREDITED" "$(jqget '.outcome' "$RC7")" "ALREADY_CREDITED"
check "核对:居民账号正常" "$(jqget '.checks.resident_account_ok' "$RC7")" "true"
check "核对:回收员已提交" "$(jqget '.checks.collector_submitted' "$RC7")" "true"
check "核对:结算单SUCCESS" "$(jqget '.checks.settlement_status' "$RC7")" "SUCCESS"
check "核对:积分规则匹配" "$(jqget '.checks.rule.match' "$RC7")" "true"
check "核对:流水来源AUTO_REISSUE" "$(jqget '.entry.source' "$RC7")" "AUTO_REISSUE"

echo "=== 23. 回收员未提交 -> 待处理任务并影响结算 ==="
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"paper\",\"est_weight_kg\":4}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-8\"}")
OID8=$(jqget '.order.id' "$RESP")
wait_status "$OID8" "$C1" ASSIGNED >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8/accept" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8/start" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8/arrive" -H "$C1" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$(jqget '.order.verify_code' "$RESP")\"}" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"paper","weight_kg":4}]}' >/dev/null
curl -s -X POST "$BASE/orders/$OID8/confirm" -H "$R1" >/dev/null
# 回收员故意不结算; 定时扫描(宽限2s)生成 OPEN 任务
TASK8=""
for i in $(seq 1 20); do
  TASK8=$(curl -s "$BASE/collector/tasks?status=OPEN" -H "$C1" \
    | jq -r --arg o "$OID8" '.tasks[]|select(.order_id==($o|tonumber))|.id' | head -1)
  [ -n "$TASK8" ] && break; sleep 1
done
[ -n "$TASK8" ] && ok "未提交任务已生成 #$TASK8" || bad "未提交任务缺失"
IMPACT=$(curl -s "$BASE/collector/tasks?status=OPEN" -H "$C1" \
  | jq -r --arg o "$OID8" '.tasks[]|select(.order_id==($o|tonumber))|.settlement_impact' | head -1)
check "任务影响结算=PENDING" "$IMPACT" "PENDING"
STTASKS=$(curl -s "$BASE/street/collector-tasks?status=OPEN" -H "$ST" | jq -r --arg t "$TASK8" '[.tasks[]|select(.id==($t|tonumber))]|length')
check "运营可见待处理任务" "$STTASKS" "1"
# 回收员按任务补提交(积分); 消费者暂停 -> 自动补发后任务 SUBMITTED, 收益冻结
curl -s -X POST "$BASE/collector/tasks/$TASK8/submit" -H "$C1" -H "$CT" -d '{"type":"points"}' >/dev/null
TSUB=""
for i in $(seq 1 25); do
  TSUB=$(curl -s "$BASE/collector/tasks" -H "$C1" | jq -r --arg t "$TASK8" '.tasks[]|select(.id==($t|tonumber))|.status' | head -1)
  [ "$TSUB" = "SUBMITTED" ] && break; sleep 1
done
check "补提交后任务=SUBMITTED" "$TSUB" "SUBMITTED"
HELD=$(curl -s "$BASE/collector/earnings" -H "$C1" | jq -r --arg o "$OID8" '.earnings[]|select(.order_id==($o|tonumber))|.held')
check "本单收益已冻结 held=true" "$HELD" "true"
# 运营核对无误: 解冻收益, 任务结案
RT=$(curl -s -X POST "$BASE/street/collector-tasks/$TASK8/resolve" -H "$ST" -H "$CT" \
  -d '{"action":"release","resolution":"回收员已补提交且积分已补发, 解除冻结"}')
check "运营结案=RESOLVED" "$(jqget '.task.status' "$RT")" "RESOLVED"
check "结案影响=RELEASED" "$(jqget '.task.settlement_impact' "$RT")" "RELEASED"
HELD2=$(curl -s "$BASE/collector/earnings" -H "$C1" | jq -r --arg o "$OID8" '.earnings[]|select(.order_id==($o|tonumber))|.held')
check "收益冻结已解除" "$HELD2" "false"

# 23b. 居民报告未提交 -> 立即开任务; 回收员改现金补结算 -> 任务结案
RESP=$(curl -s -X POST "$BASE/orders" -H "$R1" -H "$CT" -d "{
  \"items\":[{\"category\":\"plastic\",\"est_weight_kg\":2}],
  \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"e2e-order-8b\"}")
OID8B=$(jqget '.order.id' "$RESP")
wait_status "$OID8B" "$C1" ASSIGNED >/dev/null
for act in accept start arrive; do
  curl -s -X POST "$BASE/collector/orders/$OID8B/$act" -H "$C1" >/dev/null
done
curl -s -X POST "$BASE/collector/orders/$OID8B/verify-resident" -H "$C1" -H "$CT" \
  -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$(jqget '.order.verify_code' "$RESP")\"}" >/dev/null
curl -s -X POST "$BASE/collector/orders/$OID8B/weigh" -H "$C1" -H "$CT" \
  -d '{"weights":[{"category":"plastic","weight_kg":2}]}' >/dev/null
curl -s -X POST "$BASE/orders/$OID8B/confirm" -H "$R1" >/dev/null
RC8=$(curl -s -X POST "$BASE/points/reconcile" -H "$R1" -H "$CT" -d "{\"order_id\":$OID8B}")
check "居民报告:任务已开" "$(jqget '.outcome' "$RC8")" "TASK_OPENED"
TASK8B=$(jqget '.task_id' "$RC8")
CS=$(curl -s -X POST "$BASE/collector/orders/$OID8B/settle" -H "$C1" -H "$CT" -d '{"type":"cash"}')
check "现金补结算成功" "$(jqget '.settlement.status' "$CS")" "SUCCESS"
T8B_STATUS=$(curl -s "$BASE/collector/tasks" -H "$C1" | jq -r --arg t "$TASK8B" '.tasks[]|select(.id==($t|tonumber))|.status' | head -1)
check "现金补结算后任务结案" "$T8B_STATUS" "RESOLVED"

echo "=== 24. 消息回调频繁失败 -> 技术排查记录 + 受影响居民名单 ==="
kill "$APP_PID" 2>/dev/null; sleep 1
POINTS_CONSUMER_PAUSED=0 NOTIFY_FAIL_ATTEMPTS=1 CALLBACK_FAIL_THRESHOLD=3 CALLBACK_FAIL_WINDOW_SECONDS=300 \
  POINTS_DELAY_GRACE_SECONDS=2 MISSING_SETTLE_GRACE_SECONDS=2 \
  /tmp/recycle-app all > /tmp/recycle-app4.log 2>&1 &
APP_PID=$!
for i in $(seq 1 20); do curl -sf "http://127.0.0.1:18080/healthz" >/dev/null 2>&1 && break; sleep 1; done
# 两个居民各跑一单积分结算+完成(每单产生 POINTS_CREDITED 与 ORDER_COMPLETED 两条通知, 均先失败一次)
quick_order() { # $1=resident-header $2=category $3=weight $4=idem $5=verify(SCAN|ASSISTED)
  local r o v
  o=$(curl -s -X POST "$BASE/orders" -H "$1" -H "$CT" -d "{
    \"items\":[{\"category\":\"$2\",\"est_weight_kg\":$3}],
    \"time_window_start\":\"$WS\",\"time_window_end\":\"$WE\",\"idempotency_key\":\"$4\"}")
  local oid vcode
  oid=$(echo "$o" | jq -r '.order.id'); vcode=$(echo "$o" | jq -r '.order.verify_code')
  wait_status "$oid" "$C1" ASSIGNED >/dev/null
  for act in accept start arrive; do curl -s -X POST "$BASE/collector/orders/$oid/$act" -H "$C1" >/dev/null; done
  if [ "$5" = "ASSISTED" ]; then
    curl -s -X POST "$BASE/collector/orders/$oid/verify-resident" -H "$C1" -H "$CT" \
      -d '{"method":"ASSISTED","assisted_note":"老人口头确认"}' >/dev/null
  else
    curl -s -X POST "$BASE/collector/orders/$oid/verify-resident" -H "$C1" -H "$CT" \
      -d "{\"method\":\"SCAN_CODE\",\"verify_code\":\"$vcode\"}" >/dev/null
  fi
  curl -s -X POST "$BASE/collector/orders/$oid/weigh" -H "$C1" -H "$CT" \
    -d "{\"weights\":[{\"category\":\"$2\",\"weight_kg\":$3}]}" >/dev/null
  if [ "$5" = "ASSISTED" ]; then
    curl -s -X POST "$BASE/collector/orders/$oid/confirm-assisted" -H "$C1" -H "$CT" \
      -d '{"assisted_note":"确认"}' >/dev/null
  else
    curl -s -X POST "$BASE/orders/$oid/confirm" -H "$1" >/dev/null
  fi
  curl -s -X POST "$BASE/collector/orders/$oid/settle" -H "$C1" -H "$CT" -d '{"type":"points"}' >/dev/null
  wait_status "$oid" "$1" SETTLED >/dev/null
  curl -s -X POST "$BASE/collector/orders/$oid/deliver" -H "$C1" >/dev/null
  curl -s -X POST "$BASE/station/inbound/$oid/reweigh" -H "$S1" -H "$CT" \
    -d "{\"weights\":[{\"category\":\"$2\",\"weight_kg\":$3}]}" >/dev/null
  echo "$oid"
}
quick_order "$R1" paper 1 e2e-order-9 SCAN >/dev/null
quick_order "$R2" clothing 1 e2e-order-10 ASSISTED >/dev/null
# 等待技术排查记录自动开单, 且两位居民的通知都经历失败并入名单(通知经 relay 异步投递)
INC=""; HAS_R1B="false"; HAS_R2B="false"
for i in $(seq 1 40); do
  INC=$(curl -s "$BASE/street/tech-incidents?status=OPEN" -H "$ST" \
    | jq -r '.incidents[]|select(.consumer=="notification-consumer")|.id' | head -1)
  if [ -n "$INC" ]; then
    RL=$(curl -s "$BASE/street/tech-incidents/$INC/residents" -H "$ST")
    HAS_R1B=$(echo "$RL" | jq -r '[.residents[]|select(.account=="resident1")]|length>0')
    HAS_R2B=$(echo "$RL" | jq -r '[.residents[]|select(.account=="resident2")]|length>0')
    [ "$HAS_R1B" = "true" ] && [ "$HAS_R2B" = "true" ] && break
  fi
  sleep 1
done
[ -n "$INC" ] && ok "技术排查记录已生成 #$INC" || bad "技术排查记录缺失"
INCF=$(curl -s "$BASE/street/tech-incidents?status=OPEN" -H "$ST" \
  | jq -r --arg i "$INC" '.incidents[]|select(.id==($i|tonumber))|.fail_count')
[ "$INCF" -ge 3 ] && ok "排查记录失败次数=$INCF(>=3)" || bad "失败次数异常: $INCF"
INCN=$(curl -s "$BASE/street/tech-incidents?status=OPEN" -H "$ST" \
  | jq -r --arg i "$INC" '.incidents[]|select(.id==($i|tonumber))|.affected_resident_count')
[ "$INCN" -ge 2 ] && ok "受影响居民数=$INCN(>=2)" || bad "受影响居民数异常: $INCN"
# 运营获取受影响居民名单(便于集中解释)
RLIST=$(curl -s "$BASE/street/tech-incidents/$INC/residents" -H "$ST")
HAS_R1=$(echo "$RLIST" | jq -r '[.residents[]|select(.account=="resident1")]|length>0')
HAS_R2=$(echo "$RLIST" | jq -r '[.residents[]|select(.account=="resident2")]|length>0')
check "名单含张阿姨" "$HAS_R1" "true"
check "名单含王大爷" "$HAS_R2" "true"
HAS_ORDERNO=$(echo "$RLIST" | jq -r '[.residents[]|select(.order_no != "")]|length>=2')
check "名单含原订单编号" "$HAS_ORDERNO" "true"
# 回调失败明细
CFN=$(curl -s "$BASE/street/callback-failures?consumer=notification-consumer" -H "$ST" | jq -r '.failures|length')
[ "$CFN" -ge 3 ] && ok "回调失败明细 $CFN 条" || bad "回调失败明细不足: $CFN"
# 运营收到排查通知(该通知本身也失败一次后送达)
OPSN=$(curl -s "$BASE/notifications" -H "$ST" | jq -r '[.notifications[]|select(.kind=="TECH_INCIDENT_OPENED")]|length')
[ "$OPSN" -ge 1 ] && ok "运营已收到排查通知" || bad "运营排查通知缺失"
# 先失败后恢复: 通知最终送达
DELIV=$(curl -s "$BASE/notifications" -H "$R1" | jq -r '[.notifications[]|select(.kind=="POINTS_CREDITED")]|length')
[ "$DELIV" -ge 1 ] && ok "失败恢复后通知最终送达" || bad "通知未恢复送达"
# 运营关闭排查记录
RI2=$(curl -s -X POST "$BASE/street/tech-incidents/$INC/resolve" -H "$ST" -H "$CT" \
  -d '{"resolution":"通知服务已恢复, 失败消息全部重试成功, 已电话向名单居民集中解释"}')
check "排查记录已关闭" "$(jqget '.incident.status' "$RI2")" "RESOLVED"

echo
echo "==================================="
echo "PASS=$PASS FAIL=$FAIL"
kill "$APP_PID" 2>/dev/null
[ "$FAIL" -eq 0 ] && echo "E2E_ALL_PASS" || echo "E2E_HAS_FAILURES"
exit $FAIL
