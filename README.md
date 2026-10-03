# 区域再生资源回收服务（预约估价 · 称重 · 积分 · 入站结算）

面向居民、回收员、回收站、街道四方的再生资源回收平台。覆盖一次上门回收的完整链路：
**居民预约 → 系统派单 → 回收员上门核验称重 → 现场定价 → 现金/积分结算 → 站点二次复称 → 环保凭证**，
每个环节可追踪（谁确认、何时确认、对后续结算的影响）。

技术栈：**Go 1.19 + PostgreSQL 15 + Redis 7 + Kafka 3.9(KRaft)**，单模块多进程（`server` / `worker`）。
提供 **Dockerfile / docker-compose.yml** 一键启动（PG + Redis + Kafka + 自动迁移/种子 + 应用）。

## 目录结构

```
cmd/app/main.go          # 入口: server | worker | all | migrate | seed
internal/domain/         # 状态机、定价/积分/收益/容差纯函数、实体
internal/store/          # PostgreSQL 存取(事务、乐观锁、幂等; anomaly.go 异常对账/任务/排查)
internal/svc/            # 业务: 预约/派单/上门/称重/结算/复称/争议/申诉/积分异常对账(anomaly.go)
internal/events/         # Kafka 生产消费、事务性 outbox relay、回调失败记录
internal/httpapi/        # REST API(chi) 与鉴权(handlers_anomaly.go 积分异常/运营接口)
internal/seed/           # 测试账号与基础数据
migrations/              # 0001 全量 schema, 0002 积分异常(补发来源/任务/回调失败/技术排查)
scripts/infra.sh         # 一键准备 PG/Redis/Kafka
scripts/e2e.sh           # 端到端验证(89 项断言)
Dockerfile / docker-compose.yml  # 一键启动(PG+Redis+Kafka+迁移+种子+应用)
```

## 本地启动

### 方式 A：Docker 一键启动（推荐）

```bash
docker compose up --build -d
# 自动完成: PostgreSQL/Redis/Kafka 启动 -> 数据库迁移 -> 种子测试账号 -> API+worker
curl -s http://127.0.0.1:8080/healthz
docker compose logs -f app
```

> 说明：交付物含 `Dockerfile` 与 `docker-compose.yml`；本任务开发环境无 Docker 守护进程，
> 镜像未在本机实际构建，实际验证以"方式 B"本机进程 + `go test` + `e2e.sh` 为准（见文末验证方式）。

### 方式 B：本机进程（开发）

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

环境变量（均有默认值）：`PG_DSN` `REDIS_ADDR` `KAFKA_BROKERS` `HTTP_ADDR`
`DISPATCH_RADIUS_KM`(5) `LATE_GRACE_MINUTES`(10) `POINTS_BONUS_PM`(1100, 积分+10%激励)
`POINTS_PER_YUAN`(10) `POINTS_CONSUMER_PAUSED`(=1 暂停积分消费者，用于模拟积分不到账)。

积分异常处理相关：
`POINTS_DELAY_GRACE_SECONDS`(60, 结算单 PENDING 超时无流水判定系统延迟并自动补发)
`MISSING_SETTLE_GRACE_SECONDS`(300, CONFIRMED 超时无结算单判定回收员未提交并开任务)
`CALLBACK_FAIL_WINDOW_SECONDS`(120) / `CALLBACK_FAIL_THRESHOLD`(5, 回调失败时间窗与阈值)
`NOTIFY_FAIL_ATTEMPTS`(0, =N 时每条通知前 N 次回调强制失败，模拟频繁失败后恢复)。

## API 文档（OpenAPI 3.x + Swagger UI）

本项目为纯后端服务，接口文档随二进制内置（`go:embed`），启动后：

- Swagger UI（可在线填 Token 调试）：http://127.0.0.1:8080/docs （或 `/swagger`）
- OpenAPI 规范原文：http://127.0.0.1:8080/openapi.yaml （源文件 `internal/httpapi/openapi/openapi.yaml`）

在 Swagger UI 点 **Authorize** 输入测试 Token（如 `token-resident2`）即可直接调用。

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
bash scripts/e2e.sh  # 端到端 89 项断言: PASS=89 FAIL=0
```

`e2e.sh` 会重建数据库、起服务并跑通：预约估价(价格快照) → 幂等下单 → GEO 派单 →
接单/到达 → 核验码核验 → 临时加品类 → 称重扣杂 → 居民确认 → 积分结算(Kafka 异步入账) →
站点复称 → 环保凭证 → 全链路追踪；以及异常场景：现金支付失败重试、老人协助确认(非老人被拒)、
称重争议街道裁决、复称超差扣罚收益、积分不到账申诉补账、回收员迟到标记与取消、投诉处理、
街道参与/资源流向统计、站点入库质量、市场价版本化且老订单快照不变；本次新增：
**老人旧衣回收系统延迟定时自动补发(补发来源/到账时间/原订单编号)、回收员未提交任务与收益冻结/解冻、
居民报告未提交后现金补结算自动结案、消息回调频繁失败自动开技术排查记录与受影响居民名单(含运营关单)**。

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
- **积分账本**：`points_entries` 幂等键唯一（`settlement:{id}`），消费者/申诉补账天然防重；
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
| 积分到账异常(扩展) | 见下节"积分到账异常处理"：四方核对、系统延迟自动补发、回收员未提交任务与结算影响、回调失败技术排查 |
| 复称差异 | 超差(>5% 且 >500g)标记 OVER_TOLERANCE：扣减回收员收益、积分单自动生成差额调整流水 |
| 投诉 | 任意相关方 `POST /orders/{id}/complaints`，街道处理并留痕 |

### 积分到账异常处理（本次扩展）

老人（或任何居民）完成回收（如旧衣回收）后积分未到账时，服务按**四方核对**判定原因并自动处置：

1. **居民账号**：居民档案与积分账户是否正常；
2. **回收员提交**：订单是否已有结算单、结算单状态；
3. **积分规则**：按现场金额 × 每元积分 × 激励千分比重算应得积分，与结算单核对；
4. **消息回调**：该单相关的回调失败次数、是否已关联技术排查记录。

| 判定 | 系统处理 | 居民/回收员/运营可见 |
|---|---|---|
| **系统延迟**（结算单 PENDING 超时、积分流水缺失） | 定时器（默认 60s 宽限）或居民发起 `POST /points/reconcile` 时**自动补发**，结算单置 SUCCESS、订单推进 SETTLED，幂等键 `settlement:{id}` 防重 | 居民收到补发通知；`GET /points/reissues` 与余额流水均显示**补发来源**（系统延迟自动补发/申诉核对补发）、**到账时间**、**原订单编号** |
| **回收员未提交**（订单 CONFIRMED 超时无结算单） | 生成 `collector_tasks` 待处理任务（OPEN/PENDING），催办回收员；**本单结算挂起** | 回收员 `GET /collector/tasks` 可见任务；补提交后任务转 SUBMITTED 且该单收益 `held=true` **冻结**，运营核对后 `resolve` 解冻（RELEASED）；改现金补结算则直接结案 |
| **消息回调频繁失败** | 每次失败落 `callback_failures`；时间窗内失败达阈（默认 120s/5 次）自动开 `tech_incidents` 技术排查记录，且在记录 OPEN 期间持续并入受影响居民 | 运营（街道）`GET /street/tech-incidents` 看排查单，`GET /street/tech-incidents/{id}/residents` 导出**受影响居民名单**（账号/姓名/电话/原订单号/失败次数），便于集中解释；`POST .../resolve` 关单 |

关键一致性：自动补发与自动扫描/消费者正常入账用同一幂等键（`settlement:{id}`），补发后同步写消费者去重标记，
消费者恢复重试不会重复入账或重复通知；任务对（回收员, 订单, 类型）有唯一约束，扫描与居民报告并发也只开一单。

### 可追踪（`GET /api/v1/orders/{id}/trace`）

返回订单 + 明细(三段重量) + 状态事件流 + 确认记录(谁/何时/何种方式) + 结算单 +
积分流水 + 入库记录 + 环保凭证 + 回收员收益 + 争议/投诉，完整回答
“谁确认、何时确认、对后续结算的影响”。

## API 一览（前缀 `/api/v1`）

- 公共：`GET /me` `GET /prices` `GET /notifications` `GET /orders/{id}` `GET /orders/{id}/trace` `POST /orders/{id}/complaints`
- 居民：`POST /orders` `GET /orders` `POST /orders/{id}/cancel|confirm|disputes` `GET /points/balance` `POST /points/claims` `POST /points/reconcile` `GET /points/reissues` `GET /certificates`
- 回收员：`POST /collector/duty` `GET /collector/orders` `POST /collector/orders/{id}/accept|start|arrive|verify-resident|items|weigh|confirm-assisted|settle|settle/retry|deliver` `GET /collector/earnings` `GET /collector/tasks` `POST /collector/tasks/{id}/submit`
- 站点：`POST /station/inbound/{orderId}/reweigh` `GET /station/inbound` `GET /station/quality`
- 街道：`GET /street/stats` `GET /street/flows` `GET /street/disputes` `POST /street/disputes/{id}/resolve` `GET /street/complaints` `POST /street/complaints/{id}/resolve` `POST /street/prices`
- 运营(积分异常)：`GET /street/collector-tasks` `POST /street/collector-tasks/{id}/resolve` `GET /street/tech-incidents` `GET /street/tech-incidents/{id}/residents` `POST /street/tech-incidents/{id}/resolve` `GET /street/callback-failures`

## Kafka topics

`recycle.order.events`（状态机事件）· `recycle.points.commands`（积分入账指令）·
`recycle.points.events`（入账结果）· `recycle.notifications`（通知）· `recycle.station.events`

消费者组：`points-consumer`（积分入账并推进 SETTLED）、`notification-consumer`（通知落库）、
`stats-consumer`（订单完成→街道分品类统计聚合）。
