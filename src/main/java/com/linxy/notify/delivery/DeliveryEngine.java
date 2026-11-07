package com.linxy.notify.delivery;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.DeliveryAttemptRepository;
import com.linxy.notify.infra.NotificationRepository;
import com.linxy.notify.infra.SupplierRegistry;
import com.linxy.notify.metrics.Metrics;
import com.linxy.notify.supplier.Renderer;
import com.linxy.notify.supplier.SupplierSpec;

import jakarta.annotation.PreDestroy;

/**
 * 投递引擎 —— 本服务的核心（对应 mockingbird 消费者 + 发送骨架）。
 *
 * 流程：定时扫描到期通知 → 供应商限速检查 → 模板渲染 → HTTP 投递 →
 *       结果分类 → 状态机推进（成功/退避重试/死信）→ 全程审计。
 *
 * 重试策略（对齐 delayLevel 思想）：
 *   退避梯度固定序列 1s,5s,10s,30s,60s,300s,900s...（供应商可覆盖），
 *   attempt 超过梯度长度后按最后一级；耗尽 max_attempts 进 DEAD，绝不静默丢弃。
 *
 * 防惊群：批量捞取后随机抖动 0~500ms 再投递；限速桶满则本轮跳过不消耗 attempt。
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NotificationRepository notifications;
    private final DeliveryAttemptRepository attempts;
    private final SupplierRegistry registry;
    private final HttpDeliveryClient client;
    private final RateLimiterRegistry rateLimiters;
    private final Metrics metrics;
    private final SecretsProvider secrets;

    private final int batchSize;
    private final String dedupHeader;
    private final List<Integer> globalBackoff;
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private volatile boolean running = false;

    public DeliveryEngine(NotificationRepository notifications,
                          DeliveryAttemptRepository attempts,
                          SupplierRegistry registry,
                          HttpDeliveryClient client,
                          RateLimiterRegistry rateLimiters,
                          Metrics metrics,
                          SecretsProvider secrets,
                          @Value("${notify.delivery.batch-size:100}") int batchSize,
                          @Value("${notify.retry.backoff-seconds:1,5,10,30,60,300,900}") String backoff,
                          @Value("${notify.retry.idempotency-header:X-Notify-Dedup-Key}") String dedupHeader) {
        this.notifications = notifications;
        this.attempts = attempts;
        this.registry = registry;
        this.client = client;
        this.rateLimiters = rateLimiters;
        this.metrics = metrics;
        this.secrets = secrets;
        this.batchSize = batchSize;
        this.dedupHeader = dedupHeader;
        this.globalBackoff = java.util.Arrays.stream(backoff.split(",")).map(s -> Integer.parseInt(s.trim())).toList();
    }

    @Scheduled(fixedDelayString = "${notify.delivery.poll-interval-ms:1000}")
    public void poll() {
        if (running) return; // 单轮互斥：上一轮没跑完不叠加
        running = true;
        try {
            registry.reloadIfStale();
            List<Notification> due = notifications.findDue(batchSize, Instant.now());
            for (Notification n : due) {
                worker.submit(() -> {
                    try {
                        deliverOne(n);
                    } catch (Exception e) {
                        log.error("deliverOne failed id={}", n.getId(), e);
                        metrics.deliveryCounter("error");
                    }
                });
            }
            if (!due.isEmpty()) log.debug("scheduled {} notifications", due.size());
        } finally {
            running = false;
        }
    }

    void deliverOne(Notification n) {
        SupplierSpec spec = registry.get(n.getSupplier());
        if (spec == null) {
            // 供应商定义消失（配置回滚/误删）：不消耗 attempt，短暂推迟，告警
            log.error("supplier {} not found for notification {}", n.getSupplier(), n.getId());
            notifications.updateStatus(n.getId(), Notification.Status.RETRYING,
                    Instant.now().plusSeconds(30), "supplier spec missing");
            metrics.deliveryCounter("supplier_missing");
            return;
        }

        // 限速：桶满 → 本轮跳过（不消耗 attempt，避免"限速吃掉重试预算"）
        if (!rateLimiters.tryAcquire(spec.name(), spec.qps(), spec.burst())) {
            notifications.updateStatus(n.getId(), Notification.Status.RETRYING,
                    Instant.now().plusSeconds(2), "rate limited locally");
            return;
        }

        // 渲染
        HttpDeliveryClient.SupplierRequest request;
        String digest;
        try {
            Map<String, String> meta = Map.of(
                    "notification_id", n.getId(),
                    "dedup_key", n.getDedupKey() == null ? "" : n.getDedupKey(),
                    "attempt", String.valueOf(n.getAttempt() + 1));
            Renderer.RenderedRequest rendered = Renderer.render(spec, n.getEventType(), n.getPayload(),
                    secrets.forSupplier(spec.name()), meta);
            digest = sha256Short(rendered.body());
            request = new HttpDeliveryClient.SupplierRequest(
                    rendered.method(), rendered.url(), rendered.headers(), rendered.body(),
                    dedupHeader,
                    n.getDedupKey() != null ? n.getDedupKey() : n.getId(),
                    spec.connectTimeoutMs(), spec.readTimeoutMs());
        } catch (Renderer.RenderException e) {
            // 渲染失败 = 参数问题，重试无意义 → 直接死信（对齐"参数 4xx 不重试"）
            audit(n, null, "RENDER", "NON_RETRYABLE", e.getMessage(), 0);
            notifications.recordAttempt(n.getId(), Notification.Status.DEAD, null, "render: " + e.getMessage());
            metrics.deliveryCounter("render_fatal");
            return;
        }

        // 防惊群抖动
        sleepQuietly(ThreadLocalRandom.current().nextInt(0, 100));

        // 投递
        HttpDeliveryClient.DeliveryResult result = client.deliver(request);
        Outcome.Result oc = result.outcome();

        audit(n, request.url(), digest, oc.kind().name(),
                oc.httpStatus() == null ? oc.note() : oc.httpStatus() + " " + oc.note(),
                result.durationMs());

        metrics.deliveryDuration(spec.name(), result.durationMs());
        metrics.deliveryCounter(oc.kind().name().toLowerCase());

        switch (oc.kind()) {
            case SUCCESS -> {
                notifications.markDelivered(n.getId(), Instant.now());
                log.info("delivered id={} supplier={} status={} attempt={}",
                        n.getId(), n.getSupplier(), oc.httpStatus(), n.getAttempt() + 1);
            }
            case RETRYABLE, TRANSPORT_ERROR -> scheduleRetry(n, spec, oc.note());
            case NON_RETRYABLE -> {
                notifications.recordAttempt(n.getId(), Notification.Status.DEAD, null,
                        "fatal: " + oc.httpStatus() + " " + oc.note());
                log.warn("dead (non-retryable) id={} supplier={} status={}", n.getId(), n.getSupplier(), oc.httpStatus());
            }
        }
    }

    private void scheduleRetry(Notification n, SupplierSpec spec, String reason) {
        List<Integer> backoff = (spec.backoffSeconds() == null || spec.backoffSeconds().isEmpty())
                ? globalBackoff : spec.backoffSeconds();
        int nextAttemptNo = n.getAttempt() + 1; // 本次即将记录的 attempt
        if (nextAttemptNo >= n.getMaxAttempts()) {
            notifications.recordAttempt(n.getId(), Notification.Status.DEAD, null, "exhausted: " + reason);
            log.warn("dead (exhausted) id={} attempts={}", n.getId(), nextAttemptNo);
            metrics.deliveryCounter("exhausted");
            return;
        }
        int level = Math.min(nextAttemptNo - 1, backoff.size() - 1);
        int delaySec = backoff.get(level);
        Instant next = Instant.now().plusSeconds(delaySec);
        notifications.recordAttempt(n.getId(), Notification.Status.RETRYING, next,
                reason + " (next in " + delaySec + "s)");
        log.info("retry scheduled id={} attempt={} next_at={} reason={}", n.getId(), nextAttemptNo, next, reason);
    }

    private void audit(Notification n, String endpoint, String digest, String outcome, String note, long durationMs) {
        try {
            attempts.insert(new DeliveryAttemptRepository.Attempt(
                    UUID.randomUUID().toString(), n.getId(), n.getAttempt() + 1,
                    endpoint == null ? "(render)" : endpoint, digest,
                    null, outcome, durationMs, truncate(note, 500), Instant.now()));
        } catch (Exception e) {
            log.error("audit insert failed", e);
        }
    }

    private static String sha256Short(String s) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) sb.append(String.format("%02x", d[i]));
            return sb.toString();
        } catch (Exception e) {
            return "n/a";
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    @PreDestroy
    public void shutdown() {
        worker.shutdown();
    }
}
