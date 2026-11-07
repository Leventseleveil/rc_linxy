package com.linxy.notify.delivery;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.NotificationRepository;
import com.linxy.notify.infra.SupplierRegistry;
import com.linxy.notify.metrics.Metrics;
import com.linxy.notify.supplier.Renderer;
import com.linxy.notify.supplier.SupplierSpec;

/**
 * 投递引擎 —— 状态机与出站执行（对应 mockingbird 发送骨架）。
 *
 * 触发方式（v2，MQ 驱动）：
 *   主路径：dispatch/retry 队列消费者调 deliverOne()，秒级、削峰（消费并发=8）
 *   兜底：  30s 扫描捞"到期但队列消息丢失"的通知 —— DB 是事实源，MQ 只是加速器
 *
 * 重试调度：scheduleRetry() 不再原地等待，而是发 Artemis 延迟消息
 *（_AMQ_SCHED_DELIVERY，对标 RocketMQ delayLevel）—— 消费线程立刻释放。
 */
@Component
public class DeliveryEngine {

    private static final Logger log = LoggerFactory.getLogger(DeliveryEngine.class);

    private final NotificationRepository notifications;
    private final DeliveryAttemptWriter auditWriter;
    private final SupplierRegistry registry;
    private final HttpDeliveryClient client;
    private final RateLimiterRegistry rateLimiters;
    private final Metrics metrics;
    private final SecretsProvider secrets;
    private final RetryPublisher retryPublisher;

    private final int batchSize;
    private final String dedupHeader;
    private final List<Integer> globalBackoff;

    public DeliveryEngine(NotificationRepository notifications,
                          DeliveryAttemptWriter auditWriter,
                          SupplierRegistry registry,
                          HttpDeliveryClient client,
                          RateLimiterRegistry rateLimiters,
                          Metrics metrics,
                          SecretsProvider secrets,
                          RetryPublisher retryPublisher,
                          @Value("${notify.delivery.batch-size:100}") int batchSize,
                          @Value("${notify.retry.backoff-seconds:1,5,10,30,60,300,900}") String backoff,
                          @Value("${notify.retry.idempotency-header:X-Notify-Dedup-Key}") String dedupHeader) {
        this.notifications = notifications;
        this.auditWriter = auditWriter;
        this.registry = registry;
        this.client = client;
        this.rateLimiters = rateLimiters;
        this.metrics = metrics;
        this.secrets = secrets;
        this.retryPublisher = retryPublisher;
        this.batchSize = batchSize;
        this.dedupHeader = dedupHeader;
        this.globalBackoff = java.util.Arrays.stream(backoff.split(",")).map(s -> Integer.parseInt(s.trim())).toList();
    }

    /**
     * 兜底扫描（不再是主路径）：捞到期通知，重新入 dispatch 队列。
     * 与 MQ 消费的重复由 consume() 的状态/到期检查保证幂等。
     */
    @Scheduled(fixedDelayString = "${notify.dispatch.sweep-interval-ms:30000}")
    public void sweep() {
        try {
            registry.reloadIfStale();
            List<Notification> due = notifications.findDue(batchSize, Instant.now());
            for (Notification n : due) {
                retryPublisher.publishRetry(n.getId(),
                        Math.max(0, n.getNextAttemptAt().toEpochMilli() - System.currentTimeMillis()));
            }
            if (!due.isEmpty()) {
                log.info("sweeper requeued {} due notifications", due.size());
                metrics.deliveryCounter("sweep_requeued");
            }
        } catch (Exception e) {
            log.error("sweep failed", e);
        }
    }

    /** 单条投递：抢占租约 → 限速 → 渲染 → HTTP → 分类 → 状态机推进。可被消费者并发调用。 */
    public void deliverOne(Notification n) {
        // 抢占租约：MQ 消费与扫描兜底并发触发同一条通知时，只有一个赢家
        if (!notifications.tryLease(n.getId(), 60)) {
            log.debug("lease not acquired, another worker is delivering id={}", n.getId());
            return;
        }

        SupplierSpec spec = registry.get(n.getSupplier());
        if (spec == null) {
            // 供应商定义消失（配置回滚/误删）：不消耗 attempt，推迟重试并告警
            log.error("supplier {} not found for notification {}", n.getSupplier(), n.getId());
            notifications.updateStatus(n.getId(), Notification.Status.RETRYING,
                    Instant.now().plusSeconds(30), "supplier spec missing");
            retryPublisher.publishRetry(n.getId(), 30_000);
            metrics.deliveryCounter("supplier_missing");
            return;
        }

        // 限速：桶满 → 短暂延迟重新入队（不消耗 attempt，避免"限速吃掉重试预算"）
        if (!rateLimiters.tryAcquire(spec.name(), spec.qps(), spec.burst())) {
            notifications.updateStatus(n.getId(), Notification.Status.RETRYING,
                    Instant.now().plusSeconds(2), "rate limited locally");
            retryPublisher.publishRetry(n.getId(), 2_000);
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
            // 渲染失败 = 参数问题，重试无意义 → 直接死信
            auditWriter.write(n, null, "RENDER", "NON_RETRYABLE", e.getMessage(), 0);
            notifications.recordAttempt(n.getId(), Notification.Status.DEAD, null, "render: " + e.getMessage());
            metrics.deliveryCounter("render_fatal");
            return;
        }

        // 投递
        HttpDeliveryClient.DeliveryResult result = client.deliver(request);
        Outcome.Result oc = result.outcome();

        auditWriter.write(n, request.url(), digest, oc.kind().name(),
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

    /** 按退避梯度发延迟消息；耗尽 max_attempts 进 DEAD，绝不静默丢弃 */
    public void publishRetry(String id, long delayMs) {
        retryPublisher.publishRetry(id, delayMs);
    }

    private void scheduleRetry(Notification n, SupplierSpec spec, String reason) {
        List<Integer> backoff = (spec.backoffSeconds() == null || spec.backoffSeconds().isEmpty())
                ? globalBackoff : spec.backoffSeconds();
        int nextAttemptNo = n.getAttempt() + 1;
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
        retryPublisher.publishRetry(n.getId(), delaySec * 1000L);
        log.info("retry scheduled id={} attempt={} next_in={}s reason={}", n.getId(), nextAttemptNo, delaySec, reason);
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
}
