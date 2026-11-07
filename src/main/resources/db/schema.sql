-- 通知主表：业务系统提交的通知请求
CREATE TABLE IF NOT EXISTS notification (
    id            TEXT PRIMARY KEY,                -- 服务端生成 ID (ULID)
    source        TEXT NOT NULL,                   -- 来源业务系统，如 ads / crm / trade
    supplier      TEXT NOT NULL,                   -- 目标供应商标识，对应 suppliers/*.yaml 的 name
    event_type    TEXT NOT NULL,                   -- 事件类型，如 user.registered / payment.succeeded
    payload       TEXT NOT NULL,                   -- 业务事件 JSON（模板变量来源）
    dedup_key     TEXT,                            -- 业务方幂等键；同 source+dedup_key 只投一次
    status        TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/DELIVERED/RETRYING/DEAD
    attempt       INTEGER NOT NULL DEFAULT 0,      -- 已投递次数
    max_attempts  INTEGER NOT NULL DEFAULT 8,      -- 最大尝试次数（含首次）
    next_attempt_at TEXT,                          -- 下次投递时间（UTC ISO8601）
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL,
    delivered_at  TEXT,
    last_error    TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS uk_notification_dedup
    ON notification(source, dedup_key)
    WHERE dedup_key IS NOT NULL;   -- SQLite 部分索引：dedup_key 为空不参与唯一约束

CREATE INDEX IF NOT EXISTS idx_notification_due
    ON notification(status, next_attempt_at);

-- 投递审计表：每次真实 HTTP 出站都留痕（对齐 mockingbird msgTrace 思想）
CREATE TABLE IF NOT EXISTS delivery_attempt (
    id             TEXT PRIMARY KEY,
    notification_id TEXT NOT NULL,
    attempt_no     INTEGER NOT NULL,
    endpoint       TEXT NOT NULL,                 -- 实际请求 URL
    request_digest TEXT,                          -- 渲染后请求体摘要（sha256 前 16 位，避免敏感明文）
    http_status    INTEGER,                       -- HTTP 状态码；NULL 表示未收到响应（超时/连接失败）
    outcome        TEXT NOT NULL,                 -- SUCCESS/RETRYABLE/NON_RETRYABLE/TRANSPORT_ERROR
    duration_ms    INTEGER,
    error_note     TEXT,
    created_at     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_attempt_notification
    ON delivery_attempt(notification_id);
