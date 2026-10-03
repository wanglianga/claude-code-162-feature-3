-- 积分到账异常处理扩展
-- 1) 积分流水补充"补发来源/补发原因/原订单编号", 居民端可查
-- 2) 回收员未提交结算 -> 待处理任务, 结算收益冻结
-- 3) 消息回调失败记录 -> 频繁失败自动生成技术排查记录与受影响居民名单

-- ---- 积分流水扩展 ----
ALTER TABLE points_entries ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'NORMAL';
-- NORMAL(正常入账) | AUTO_REISSUE(系统自动补发) | MANUAL_REISSUE(居民申诉人工补账)
ALTER TABLE points_entries ADD COLUMN IF NOT EXISTS reissue_reason TEXT NOT NULL DEFAULT '';
-- SYSTEM_DELAY(系统延迟) | RESIDENT_CLAIM(居民申诉) | ''
ALTER TABLE points_entries ADD COLUMN IF NOT EXISTS origin_order_no TEXT NOT NULL DEFAULT '';
-- 原订单编号(冗余, 即便订单归档也可展示)

-- ---- 回收员待处理任务(回收员未提交积分结算) ----
CREATE TABLE IF NOT EXISTS collector_tasks (
    id                BIGSERIAL PRIMARY KEY,
    task_no           TEXT NOT NULL UNIQUE,
    collector_id      BIGINT NOT NULL,
    order_id          BIGINT NOT NULL REFERENCES orders(id),
    resident_id       BIGINT NOT NULL,
    kind              TEXT NOT NULL DEFAULT 'POINTS_SUBMISSION_MISSING',
    status            TEXT NOT NULL DEFAULT 'OPEN',      -- OPEN|SUBMITTED|RESOLVED|CANCELLED
    settlement_impact TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING(未结算)|HELD(已提交,收益冻结待核)|RELEASED(影响解除)
    detail            JSONB NOT NULL DEFAULT '{}',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at      TIMESTAMPTZ,
    resolved_at       TIMESTAMPTZ,
    resolution        TEXT NOT NULL DEFAULT '',
    UNIQUE (collector_id, order_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_collector_tasks_collector ON collector_tasks(collector_id, status);
CREATE INDEX IF NOT EXISTS idx_collector_tasks_status ON collector_tasks(status);

-- 回收员收益冻结标记(任务未结清前, 该单收益 held=true 不参与结算发放)
ALTER TABLE collector_earnings ADD COLUMN IF NOT EXISTS held BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE collector_earnings ADD COLUMN IF NOT EXISTS held_reason TEXT NOT NULL DEFAULT '';

-- ---- 消息回调失败明细(每次消费/投递失败一行) ----
CREATE TABLE IF NOT EXISTS callback_failures (
    id          BIGSERIAL PRIMARY KEY,
    topic       TEXT NOT NULL,
    consumer    TEXT NOT NULL,
    msg_key     TEXT NOT NULL DEFAULT '',
    attempt     INT NOT NULL DEFAULT 1,
    resident_id BIGINT NOT NULL DEFAULT 0, -- 从消息载荷提取, 用于聚合受影响居民
    order_id    BIGINT NOT NULL DEFAULT 0,
    reason      TEXT NOT NULL DEFAULT '',
    payload     JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cb_fail_consumer_time ON callback_failures(consumer, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cb_fail_key ON callback_failures(msg_key);

-- ---- 技术排查记录(回调频繁失败自动开单) ----
CREATE TABLE IF NOT EXISTS tech_incidents (
    id                       BIGSERIAL PRIMARY KEY,
    incident_no              TEXT NOT NULL UNIQUE,
    kind                     TEXT NOT NULL DEFAULT 'CALLBACK_FAIL_BURST',
    topic                    TEXT NOT NULL DEFAULT '',
    consumer                 TEXT NOT NULL DEFAULT '',
    status                   TEXT NOT NULL DEFAULT 'OPEN', -- OPEN|RESOLVED
    fail_count               INT NOT NULL DEFAULT 0,
    window_seconds           INT NOT NULL DEFAULT 0,
    threshold                INT NOT NULL DEFAULT 0,
    affected_resident_count  INT NOT NULL DEFAULT 0,
    detail                   JSONB NOT NULL DEFAULT '{}',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at              TIMESTAMPTZ,
    resolution               TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_tech_incidents_consumer ON tech_incidents(consumer, status);

-- ---- 受影响居民名单(运营集中解释用) ----
CREATE TABLE IF NOT EXISTS incident_affected_residents (
    incident_id  BIGINT NOT NULL REFERENCES tech_incidents(id) ON DELETE CASCADE,
    resident_id  BIGINT NOT NULL,
    order_id     BIGINT NOT NULL DEFAULT 0,
    fail_count   INT NOT NULL DEFAULT 1,
    last_fail_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (incident_id, resident_id, order_id)
);
CREATE INDEX IF NOT EXISTS idx_incident_residents_resident ON incident_affected_residents(resident_id);
