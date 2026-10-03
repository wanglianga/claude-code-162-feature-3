# 区域再生资源回收服务（预约估价 · 称重 · 积分 · 入站结算）

面向居民、回收员、回收站、街道四方的再生资源回收平台。覆盖一次上门回收的完整链路：
**居民预约 → 系统派单 → 回收员上门核验称重 → 现场定价 → 现金/积分结算 → 站点二次复称 → 环保凭证**，
每个环节可追踪（谁确认、何时确认、对后续结算的影响）。

技术栈：**Go 1.19 + PostgreSQL 15 + Redis 7 + Kafka 3.9(KRaft)**，单模块多进程（`server` / `worker`）。

## 一键启动（Docker）

```bash
docker compose up --build      # PG + Redis + Kafka + 自动迁移/种子 + 服务(:8080)
curl http://127.0.0.1:8080/healthz
```

## 目录结构

```
cmd/app/main.go          # 入口: server | worker | all | migrate | seed
internal/domain/         # 状态机、定价/积分/收益/容差纯函数、实体
internal/store/          # PostgreSQL 存取(事务、乐观锁、幂等)
internal/svc/            # 业务: 预约/派单/上门/称重/结算/复称/争议/申诉
internal/events/         # Kafka 生产消费、事务性 outbox relay
internal/httpapi/        # REST API(chi) 与鉴权
internal/seed/           # 测试账号与基础数据
migrations/0001_init.sql # 全量 schema
scripts/infra.sh         # 一键准备 PG/Redis/Kafka
scripts/e2e.sh           # 端到端验证(全链路 + 积分异常等全部断言)
```

## 本地启动

```bash
# 1. 基础设施: PostgreSQL@5433 + Redis@6379(系统自带) + Kafka@9092
bash scripts/infra.sh

# 2. 迁移 + 种子数据(测试账号/品类/市场价/排班/在岗)
go build -o /tmp/recycle-app ./cmd/app
/tmp/recycle-app seed

# 3a. 单进程(开发): API + 派单定时器 + Kafka relay/消费者
/tmp/recycle-app all          # 监听 :8080

# 3b. 或分进程(生产形态)
/tmp/recycle-app server       # HTTP API + 派单/迟到检测
/tmp/recycle-app worker       # outbox relay + 积分/通知/统计消费者
```

## API 文档（OpenAPI 3 + Swagger UI，离线内置）

服务启动后（无需鉴权）：

- Swagger UI：<http://127.0.0.1:8080/docs/>
- OpenAPI 3 规范：<http://127.0.0.1:8080/openapi.json>（规范文件 `internal/openapi/openapi.json`，静态资源 go:embed 内置）


环境变量（均有默认值）：`PG_DSN` `REDIS_ADDR` `KAFKA_BROKERS` `HTTP_ADDR`
`DISPATCH_RADIUS_KM`(5) `LATE_GRACE_MINUTES`(10) `POINTS_BONUS_PM`(1100, 积分+10%激励)
`POINTS_PER_YUAN`(10) `POINTS_CONSUMER_PAUSED`(=1 暂停积分消费者，用于模拟积分不到账)
`POINTS_REISSUE_GRACE_SEC`(30, 结算提交后多久未入账判定系统延迟并自动补发)
`UNSUBMITTED_GRACE_SEC`(60, 居民确认后多久未提交结算判定回收员未提交)
`CALLBACK_FAIL_WINDOW_SEC`(300) `CALLBACK_FAIL_THRESHOLD`(5, 窗口内回调失败达阈值生成技术排查记录)
`CALLBACK_RECOVER_SEC`(60, 回调静默多久判定恢复并自动关闭排查记录)。

## 测试账号（Bearer Token 即身份）

| 角色 | 账号 | Token | 说明 |
|---|---|---|---|
| 居民 | resident1 | `token-resident1` | 张阿姨，5 楼无电梯 |
| 居民(老人) | resident2 | `token-resident2` | 王大爷，`is_elderly=true` |
| 回收员 | collector1 | `token-collector1` | 李师傅，在岗 |
| 回收员 | collector2 | `token-collector2` | 赵师傅，在岗 |
| 回收站 | station1 | `token-station1` | 城东回收站 |
| 街道 | street1 | `token-street1` | 城东街道办 |

调用示例：`curl -H "Authorization: Bearer token-resident1" http://127.0.0.1:8080/api/v1/orders`

## 验证方式（以下均已实际执行通过）

```bash
go test ./...        # 领域定价/状态机单测 + 积分账本幂等集成测试(需 PG)
bash scripts/e2e.sh  # 端到端断言: 全链路 + 异常场景(含积分异常 22-27), PASS=92 FAIL=0
curl http://127.0.0.1:8080/docs/        # Swagger UI(离线内置)
curl http://127.0.0.1:8080/openapi.json # OpenAPI 3.0 规范
```

> 另提供容器一键启动（`Dockerfile` + `docker-compose.yml`，编排 PG/Redis/Kafka/应用并自动迁移种子）：
> `docker compose up --build`，然后 `curl http://127.0.0.1:8080/healthz`。
> 该方式在当前无 Docker 的验证环境中未实际执行，仅作为交付的启动配置。

`e2e.sh` 会重建数据库、起服务并跑通：预约估价(价格快照) → 幂等下单 → GEO 派单 →
接单/到达 → 核验码核验 → 临时加品类 → 称重扣杂 → 居民确认 → 积分结算(Kafka 异步入账) →
站点复称 → 环保凭证 → 全链路追踪；以及异常场景：现金支付失败重试、老人协助确认(非老人被拒)、
称重争议街道裁决、复称超差扣罚收益、积分不到账申诉补账、回收员迟到标记与取消、投诉处理、
街道参与/资源流向统计、站点入库质量、市场价版本化且老订单快照不变；
积分到账异常专项(22-27)：**老人完成旧衣回收后积分延迟，定时核对四要素后自动补发并通知
(居民端含补发来源/到账时间/原订单编号)、回收员未提交生成待处理任务并挂起结算(补提交后自动关闭、收益恢复)、
消息回调频繁失败生成技术排查记录并产出受影响居民名单(恢复后自动关闭、挂账补发)、运营手动核对与权限校验、
OpenAPI 3/Swagger UI 文档**。

## 核心设计

### 订单状态机（非法流转一律 409）

```
PENDING → ASSIGNED → ACCEPTED → EN_ROUTE → ARRIVED → RESIDENT_VERIFIED
→ WEIGHED → CONFIRMED → SETTLED → DELIVERED → INBOUND_CONFIRMED → COMPLETED
              ↘ CANCELLED(预约阶段可取消)   WEIGHED ⇄ DISPUTED(争议冻结→裁决)
```

### 价格透明

- 下单时把**市场价快照**锁进 `order_items.est_price_cents`，预估价=快照价×预计重量；
- 现场金额=现场重量×(1−扣杂率)×快照价；站点复称金额=复称重量×(1−扣杂率)×快照价；
- 市场价版本化（`market_prices`），街道调价不影响已下单快照（e2e 第 21 项验证）。

### 一致性

- **事务性 outbox**：状态变更、事件、Kafka 消息同一 DB 事务提交，relay 至少一次投递；
- **积分账本**：`points_entries` 幂等键唯一（`settlement:{id}`），消费者/申诉补账/定时自动补发共用幂等标记天然防重；
- **积分补发留痕**：`points_reissues` 记录补发来源、到账时间、原订单编号（`reissue:settlement:{id}` 唯一）；
- **回调可观测**：`callback_failures` 滑窗计数，达阈值开 `tech_incidents` 并产出受影响居民名单；
- **乐观锁**：订单 `version` 条件更新，并发操作安全失败；
- **幂等下单**：`Idempotency-Key` 唯一约束，重复提交返回原单；
- **消费者幂等**：`processed_events` 去重，重放不产生副作用。

### 异常场景 → 系统处理

| 场景 | 处理 |
|---|---|
| 临时加品类 | 上门后称重前 `POST /collector/orders/{id}/items`，按当时市场价快照计价，事件留痕 |
| 回收员迟到 | 定时器超窗+宽限标记 `late_minutes`、事件+通知居民，可取消改约 |
| 称重争议 | 居民 `POST /orders/{id}/disputes` 冻结订单，街道裁决(改重量/维持/驳回)后继续 |
| 混入不可回收物 | 称重时 `contamination_pm` 千分比扣杂，金额按有效重量计 |
| 老人不会确认 | 回收员 `ASSISTED` 协助核验/确认，强制备注留痕；非老人居民被拒 |
| 现金支付失败 | 结算单 FAILED、订单停留 CONFIRMED，`settle/retry` 重试（`simulate_fail` 为测试注入） |
| 积分不到账 | 积分消费者故障时订单停留 CONFIRMED；居民 `POST /points/claims` 同步对账补账(幂等) |
| 积分到账延迟 | 定时核对四要素后自动补发(幂等)、通知居民，居民端可见补发来源/到账时间/原订单编号 |
| 回收员未提交结算 | 超宽限未提交生成回收员待处理任务，结算挂起(收益暂不生成)，补提交后任务自动关闭 |
| 消息回调频繁失败 | 失败计数达阈值自动开技术排查记录并通知运营；恢复后自动关闭，提供受影响居民名单 |
| 复称差异 | 超差(>5% 且 >500g)标记 OVER_TOLERANCE：扣减回收员收益、积分单自动生成差额调整流水 |
| 投诉 | 任意相关方 `POST /orders/{id}/complaints`，街道处理并留痕 |

### 积分到账异常核对（四要素 → 处置）

定时器（默认 5s）与运营手动接口 `POST /street/points/reconcile` 对积分订单逐一核对：

1. **居民账号**：居民档案与积分账户是否存在；
2. **回收员提交**：`settlements` 与 `SETTLE_CONFIRM` 确认记录是否存在；
3. **积分规则**：`PointsForAmount(现场金额, 每元积分, 激励千分比)` 重算应得积分；
4. **消息回调**：积分 Kafka 指令是否被消费者成功入账（`points_entries` 幂等键）。

处置：

- **系统延迟**（已提交但回调超过 `POINTS_REISSUE_GRACE_SEC` 未入账）：以与消费者相同的
  幂等标记 `processed_events("points-consumer","settlement:{id}")` 和流水键 `settlement:{id}`
  **自动补发**，推进 SETTLED、生成回收员收益、发 `POINTS_REISSUED` 通知；消费者恢复后重放为空操作，不重复入账。
- **回收员未提交**（CONFIRMED 超 `UNSUBMITTED_GRACE_SEC` 无结算单）：写 `collector_tasks`
  待处理任务（结算挂起、收益暂不生成），通知回收员与居民；回收员补提交 `settle` 时任务自动关闭。
- **回调频繁失败**：消费者每次失败写 `callback_failures`，滑动窗口内达阈值生成
  `tech_incidents`（OPEN）并通知运营；静默超过 `CALLBACK_RECOVER_SEC` 自动关闭。
  运营通过 `GET /street/callback-affected` 获取**受影响居民名单**（账号/电话/订单号/失败次数/已补发数），便于集中解释。

居民端在 `GET /points/reissues` 与 `GET /points/balance`（补发流水带 `reissue_source`/
`credited_at`/`original_order_no`）看到**补发来源、到账时间、原订单编号**；
`GET /orders/{id}/trace` 同步展示 `points_reissues` 与 `collector_tasks`。


### 可追踪（`GET /api/v1/orders/{id}/trace`）

返回订单 + 明细(三段重量) + 状态事件流 + 确认记录(谁/何时/何种方式) + 结算单 +
积分流水 + 入库记录 + 环保凭证 + 回收员收益 + 争议/投诉，完整回答
“谁确认、何时确认、对后续结算的影响”。

## API 一览（前缀 `/api/v1`）

- 公共：`GET /me` `GET /prices` `GET /notifications` `GET /orders/{id}` `GET /orders/{id}/trace` `POST /orders/{id}/complaints`
- 居民：`POST /orders` `GET /orders` `POST /orders/{id}/cancel|confirm|disputes` `GET /points/balance` `GET /points/reissues` `POST /points/claims` `GET /points/claims` `GET /certificates`
- 回收员：`POST /collector/duty` `GET /collector/orders` `GET /collector/tasks` `POST /collector/orders/{id}/accept|start|arrive|verify-resident|items|weigh|confirm-assisted|settle|settle/retry|deliver` `GET /collector/earnings`
- 站点：`POST /station/inbound/{orderId}/reweigh` `GET /station/inbound` `GET /station/quality`
- 街道：`GET /street/stats` `GET /street/flows` `GET /street/disputes` `POST /street/disputes/{id}/resolve` `GET /street/complaints` `POST /street/complaints/{id}/resolve` `POST /street/prices`
- 运营(积分异常)：`POST /street/points/reconcile` `GET /street/orders/{id}/points-check` `GET /street/tech-incidents` `GET /street/callback-affected?since_minutes=60`

## Kafka topics

`recycle.order.events`（状态机事件）· `recycle.points.commands`（积分入账指令）·
`recycle.points.events`（入账结果）· `recycle.notifications`（通知）· `recycle.station.events`

消费者组：`points-consumer`（积分入账并推进 SETTLED）、`notification-consumer`（通知落库）、
`stats-consumer`（订单完成→街道分品类统计聚合）。
