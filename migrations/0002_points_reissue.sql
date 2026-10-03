-- 积分到账异常扩展: 自动补发 / 回收员待处理任务 / 回调失败技术排查 / 受影响居民名单
-- 金额: 分; 重量: 克; 比例: 千分比

-- 1) 积分补发记录(居民端可见: 补发来源、到账时间、原订单编号)
--    与 points_entries 的幂等键 settlement:{id} 联动, 保证一笔结算最多补发一次。
CREATE TABLE IF NOT EXISTS points_reissues (
    id                 BIGSERIAL PRIMARY KEY,
    resident_id        BIGINT NOT NULL,
    order_id           BIGINT NOT NULL,
    settlement_id      BIGINT NOT NULL,
    points             BIGINT NOT NULL,              -- 补发积分(按规则重算)
    reason             TEXT NOT NULL,                -- SYSTEM_DELAY | CALLBACK_RETRY_EXHAUSTED | MANUAL_RECONCILE
    source             TEXT NOT NULL,                -- 补发来源说明(居民端展示)
    original_order_no  TEXT NOT NULL,                -- 原订单编号(居民端展示)
    points_entry_id    BIGINT,                       -- 对应积分流水
    idempotency_key    TEXT NOT NULL UNIQUE,         -- reissue:settlement:{id}
    credited_at        TIMESTAMPTZ NOT NULL DEFAULT now(), -- 到账时间(居民端展示)
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_reissues_resident ON points_reissues(resident_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_reissues_order ON points_reissues(order_id);

-- 2) 回收员待处理任务(回收员未提交结算/称重等时生成, 影响结算)
CREATE TABLE IF NOT EXISTS collector_tasks (
    id            BIGSERIAL PRIMARY KEY,
    collector_id  BIGINT NOT NULL,
    order_id      BIGINT NOT NULL,
    kind          TEXT NOT NULL,                     -- SETTLEMENT_HELD
    status        TEXT NOT NULL DEFAULT 'OPEN',      -- OPEN | RESOLVED | EXPIRED
    reason        TEXT NOT NULL,
    impact        TEXT NOT NULL DEFAULT '',          -- 对结算的影响说明
    detail        JSONB NOT NULL DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ
);
-- 一个回收员一单一种任务最多一条未结
CREATE UNIQUE INDEX IF NOT EXISTS idx_collector_tasks_open
    ON collector_tasks(collector_id, order_id, kind) WHERE status='OPEN';
CREATE INDEX IF NOT EXISTS idx_collector_tasks_collector ON collector_tasks(collector_id, id DESC);

-- 3) 消息回调投递失败记录(每次失败一行, 技术排查用)
CREATE TABLE IF NOT EXISTS callback_failures (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    consumer     TEXT NOT NULL,
    msg_key      TEXT NOT NULL,
    err          TEXT NOT NULL,
    resident_id  BIGINT,                              -- 受影响居民(可解析时)
    order_id     BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cb_fail_created ON callback_failures(created_at);
CREATE INDEX IF NOT EXISTS idx_cb_fail_resident ON callback_failures(resident_id, id);

-- 4) 技术排查记录(回调频繁失败时自动生成)
CREATE TABLE IF NOT EXISTS tech_incidents (
    id            BIGSERIAL PRIMARY KEY,
    kind          TEXT NOT NULL,                     -- CALLBACK_FAILURE
    topic         TEXT NOT NULL,
    consumer      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'OPEN',      -- OPEN | RESOLVED
    fail_count    INT NOT NULL DEFAULT 0,
    window_secs   INT NOT NULL DEFAULT 0,
    threshold     INT NOT NULL DEFAULT 0,
    summary       TEXT NOT NULL,
    first_fail_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_fail_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- 同 topic+consumer 同时只有一条 OPEN
CREATE UNIQUE INDEX IF NOT EXISTS idx_tech_incidents_open
    ON tech_incidents(topic, consumer) WHERE status='OPEN';
CREATE INDEX IF NOT EXISTS idx_tech_incidents_status ON tech_incidents(status, id DESC);
