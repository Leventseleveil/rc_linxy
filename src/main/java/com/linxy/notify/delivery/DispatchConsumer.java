package com.linxy.notify.delivery;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.NotificationRepository;

/**
 * MQ 驱动的投递流水线（对应参考系统：mockingbird 消费 retry topic）。
 *
 * notify.dispatch：普通队列，落库后立即触发首次投递（秒级，替代纯扫描）。
 * notify.dispatch.retry：延迟队列 —— _AMQ_SCHED_DELIVERY 延迟投递，
 *                       对标 RocketMQ delayLevel：消费者不 sleep，立刻回去消费下一条。
 *
 * 消费语义：消息处理失败不影响 MQ ACK（DB 是事实源，扫描器兜底捞回），
 * 与参考系统"消费始终返回 SUCCESS + 发送层自主重试"同一思想：防重试风暴。
 */
@Component
public class DispatchConsumer {

    private static final Logger log = LoggerFactory.getLogger(DispatchConsumer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final NotificationRepository notifications;
    private final DeliveryEngine engine;

    public DispatchConsumer(NotificationRepository notifications, DeliveryEngine engine) {
        this.notifications = notifications;
        this.engine = engine;
    }

    /** 消费 dispatch / retry 两个队列（同一处理逻辑） */
    @JmsListener(destination = "${notify.dispatch.queue:notify.dispatch}")
    public void onDispatch(String body) {
        consume(body);
    }

    @JmsListener(destination = "${notify.dispatch.queue:notify.dispatch}.retry")
    public void onRetry(String body) {
        consume(body);
    }

    void consume(String body) {
        String id;
        try {
            id = MAPPER.readTree(body).get("id").asText();
        } catch (Exception e) {
            log.error("bad dispatch message: {}", body, e);
            return;
        }
        Notification n = notifications.findById(id).orElse(null);
        if (n == null) {
            log.warn("dispatch for unknown notification id={}", id);
            return;
        }
        // 状态已终态（已送达/死信）→ 重复消息直接忽略（扫描兜底与 MQ 竞态的幂等保护）
        if (n.getStatus() == Notification.Status.DELIVERED || n.getStatus() == Notification.Status.DEAD) {
            return;
        }
        // 到期时间未到（延迟消息提前到达 / 扫描与 MQ 竞态）→ 重新延迟入队
        if (n.getNextAttemptAt() != null && n.getNextAttemptAt().isAfter(Instant.now())) {
            long delayMs = n.getNextAttemptAt().toEpochMilli() - System.currentTimeMillis();
            engine.publishRetry(id, delayMs);
            return;
        }
        engine.deliverOne(n);
    }
}
