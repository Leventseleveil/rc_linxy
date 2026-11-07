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
        return java.util.Map.of(
                "accepted", accepted.get(),
                "delivered", delivered.get(),
                "retryable", retryable.get(),
                "non_retryable", nonRetryable.get(),
                "transport_error", transportError.get(),
                "exhausted_dead", exhausted.get(),
                "supplier_missing", supplierMissing.get(),
                "render_fatal", renderFatal.get(),
                "delivery_p95_ms", p95DurationMs());
    }
}
