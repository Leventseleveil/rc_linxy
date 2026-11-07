package com.linxy.notify.domain;

import java.time.Instant;

/**
 * 通知聚合根。
 * 状态机：PENDING → DELIVERED（成功）
 *              ↘ RETRYING（可重试失败，按退避梯度回到队列）
 *                    ↘ DEAD（重试耗尽，死信，可人工重发）
 *         PENDING → DEAD（不可重试失败，如 4xx 参数错误）
 */
public class Notification {
    private String id;
    private String source;
    private String supplier;
    private String eventType;
    private String payload;      // 原始业务事件 JSON
    private String dedupKey;
    private Status status;
    private int attempt;
    private int maxAttempts;
    private Instant nextAttemptAt;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant deliveredAt;
    private String lastError;

    public enum Status { PENDING, RETRYING, DELIVERED, DEAD }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public String getSupplier() { return supplier; }
    public void setSupplier(String supplier) { this.supplier = supplier; }
    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public String getDedupKey() { return dedupKey; }
    public void setDedupKey(String dedupKey) { this.dedupKey = dedupKey; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public int getAttempt() { return attempt; }
    public void setAttempt(int attempt) { this.attempt = attempt; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public Instant getNextAttemptAt() { return nextAttemptAt; }
    public void setNextAttemptAt(Instant nextAttemptAt) { this.nextAttemptAt = nextAttemptAt; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public Instant getDeliveredAt() { return deliveredAt; }
    public void setDeliveredAt(Instant deliveredAt) { this.deliveredAt = deliveredAt; }
    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }
}
