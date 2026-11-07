package com.linxy.notify.delivery;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.DeliveryAttemptRepository;

/**
 * 审计写入器：每次出站尝试一条记录（对齐 mockingbird msgTrace —— "发没发、发了几次、对方回了什么"）。
 * 单独抽类：写审计失败不阻塞投递主流程，只记错误日志。
 */
@Component
public class DeliveryAttemptWriter {

    private static final Logger log = LoggerFactory.getLogger(DeliveryAttemptWriter.class);

    private final DeliveryAttemptRepository attempts;

    public DeliveryAttemptWriter(DeliveryAttemptRepository attempts) {
        this.attempts = attempts;
    }

    public void write(Notification n, String endpoint, String digest, String outcome, String note, long durationMs) {
        try {
            attempts.insert(new DeliveryAttemptRepository.Attempt(
                    UUID.randomUUID().toString(), n.getId(), n.getAttempt() + 1,
                    endpoint == null ? "(render)" : endpoint, digest,
                    null, outcome, durationMs, truncate(note, 500), Instant.now()));
        } catch (Exception e) {
            log.error("audit insert failed", e);
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
