package com.linxy.notify.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

/**
 * 极简指标（无 Micrometer 依赖树时的替代）。
 * 同时暴露到 /api/v1/metrics 供外部抓取；接 Prometheus 时换 Micrometer 一行接入。
 */
@Component
public class Metrics {

    private final AtomicLong delivered = new AtomicLong();
    private final AtomicLong retryable = new AtomicLong();
    private final AtomicLong nonRetryable = new AtomicLong();
    private final AtomicLong transportError = new AtomicLong();
    private final AtomicLong exhausted = new AtomicLong();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong supplierMissing = new AtomicLong();
    private final AtomicLong renderFatal = new AtomicLong();
    private final AtomicLong dispatchPublishFailed = new AtomicLong();
    private final AtomicLong retryPublishFailed = new AtomicLong();
    private final AtomicLong sweepRequeued = new AtomicLong();

    // p95 简易实现：环形桶记录最近 1024 次耗时
    private final long[] durations = new long[1024];
    private int durationIdx = 0;

    public void deliveryCounter(String kind) {
        switch (kind) {
            case "success" -> delivered.incrementAndGet();
            case "retryable" -> retryable.incrementAndGet();
            case "non_retryable" -> nonRetryable.incrementAndGet();
            case "transport_error" -> transportError.incrementAndGet();
            case "exhausted" -> exhausted.incrementAndGet();
            case "accepted" -> accepted.incrementAndGet();
            case "supplier_missing" -> supplierMissing.incrementAndGet();
            case "render_fatal" -> renderFatal.incrementAndGet();
            case "dispatch_publish_failed" -> dispatchPublishFailed.incrementAndGet();
            case "retry_publish_failed" -> retryPublishFailed.incrementAndGet();
            case "sweep_requeued" -> sweepRequeued.incrementAndGet();
            default -> {}
        }
    }

    public synchronized void deliveryDuration(String supplier, long ms) {
        durations[durationIdx++ % durations.length] = ms;
    }

    public synchronized long p95DurationMs() {
        int filled = Math.min(durationIdx, durations.length);
        if (filled == 0) return 0;
        long[] copy = new long[filled];
        System.arraycopy(durations, 0, copy, 0, filled);
        java.util.Arrays.sort(copy);
        return copy[(int) Math.floor(filled * 0.95)];
    }

    public java.util.Map<String, Object> snapshot() {
        java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("accepted", accepted.get());
        m.put("delivered", delivered.get());
        m.put("retryable", retryable.get());
        m.put("non_retryable", nonRetryable.get());
        m.put("transport_error", transportError.get());
        m.put("exhausted_dead", exhausted.get());
        m.put("supplier_missing", supplierMissing.get());
        m.put("render_fatal", renderFatal.get());
        m.put("dispatch_publish_failed", dispatchPublishFailed.get());
        m.put("retry_publish_failed", retryPublishFailed.get());
        m.put("sweep_requeued", sweepRequeued.get());
        m.put("delivery_p95_ms", p95DurationMs());
        return m;
    }
}
