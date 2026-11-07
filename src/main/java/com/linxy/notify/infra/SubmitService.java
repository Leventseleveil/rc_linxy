package com.linxy.notify.infra;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linxy.notify.domain.Notification;
import com.linxy.notify.metrics.Metrics;

/**
 * 接入服务：两条入口汇聚到同一条"落库 → 发 dispatch 消息"路径。
 *
 * 入口 A：业务系统同步 HTTP POST /api/v1/notifications（本服务 ACK 202）
 * 入口 B：业务系统往自己的 intake 队列发 JMS 消息（notify.intake.<source>）
 *
 * 对应参考系统：peregrine 同时消费多业务 MQ + 提供 HTTP 接口。
 * 可靠性关键顺序：先 INSERT（事实源），成功后再发 MQ —— MQ 丢了有 30s 扫描兜底，
 * MQ 在了也只是"加速触发"，不是正确性的必要条件。
 */
@Service
public class SubmitService {

    private static final Logger log = LoggerFactory.getLogger(SubmitService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NotificationRepository notifications;
    private final SupplierRegistry registry;
    private final JmsTemplate jms;
    private final Metrics metrics;
    private final String dispatchQueue;

    public SubmitService(NotificationRepository notifications,
                         SupplierRegistry registry,
                         JmsTemplate jms,
                         Metrics metrics,
                         @Value("${notify.dispatch.queue:notify.dispatch}") String dispatchQueue) {
        this.notifications = notifications;
        this.registry = registry;
        this.jms = jms;
        this.metrics = metrics;
        this.dispatchQueue = dispatchQueue;
    }

    /** 入口 A/B 的统一载荷 */
    public record IntakeMessage(String supplier, String eventType, String payload, String dedupKey) {}

    /**
     * 处理一条接入消息。返回结果供 HTTP 层转响应；MQ 消费层忽略返回值（失败只记日志，
     * 消息不重投 —— DB 才是事实源，扫描器会兜底捞回 PENDING）。
     */
    public SubmitOutcome accept(String source, IntakeMessage msg) {
        if (registry.get(msg.supplier()) == null) {
            return SubmitOutcome.reject("unknown supplier: " + msg.supplier());
        }
        if (msg.payload() == null || msg.payload().isBlank()) {
            return SubmitOutcome.reject("payload required (JSON)");
        }

        Notification n = new Notification();
        n.setId(UUID.randomUUID().toString());
        n.setSource(source == null || source.isBlank() ? "biz-system" : source);
        n.setSupplier(msg.supplier());
        n.setEventType(msg.eventType());
        n.setPayload(msg.payload());
        n.setDedupKey(msg.dedupKey());
        n.setStatus(Notification.Status.PENDING);
        n.setAttempt(0);
        n.setMaxAttempts(8);
        n.setNextAttemptAt(Instant.now());
        n.setCreatedAt(Instant.now());
        n.setUpdatedAt(Instant.now());

        boolean inserted = notifications.insert(n);   // ① 先落库（事实源）
        metrics.deliveryCounter("accepted");
        if (!inserted) {
            Notification existing = msg.dedupKey() == null ? null
                    : notifications.findByDedup(n.getSource(), msg.dedupKey()).orElse(null);
            if (existing != null) {
                return SubmitOutcome.duplicated(existing);
            }
            return SubmitOutcome.reject("insert conflict");
        }

        try {                                          // ② 再发 MQ（加速触发，允许失败）
            String body = MAPPER.writeValueAsString(Map.of("id", n.getId()));
            jms.send(dispatchQueue, s -> s.createTextMessage(body));
        } catch (Exception e) {
            log.warn("dispatch publish failed (sweeper will pick it up), id={}", n.getId(), e);
            metrics.deliveryCounter("dispatch_publish_failed");
        }
        return SubmitOutcome.accepted(n);
    }

    public record SubmitOutcome(String kind, Notification notification, String error) {
        static SubmitOutcome accepted(Notification n) { return new SubmitOutcome("ACCEPTED", n, null); }
        static SubmitOutcome duplicated(Notification n) { return new SubmitOutcome("DUPLICATED", n, null); }
        static SubmitOutcome reject(String err) { return new SubmitOutcome("REJECTED", null, err); }
        public boolean accepted() { return "ACCEPTED".equals(kind); }
        public boolean duplicated() { return "DUPLICATED".equals(kind); }
    }
}
