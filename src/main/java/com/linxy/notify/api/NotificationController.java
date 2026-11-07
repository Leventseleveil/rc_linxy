package com.linxy.notify.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.DeliveryAttemptRepository;
import com.linxy.notify.infra.NotificationRepository;
import com.linxy.notify.infra.SupplierRegistry;
import com.linxy.notify.metrics.Metrics;

import jakarta.validation.Valid;

/**
 * 接入 API（对应 peregrine 接入层）：同步 HTTP 收请求、异步投递。
 * 提交即落库返回 202 + notification_id —— 业务方与外部 API 完全解耦。
 */
@RestController
@RequestMapping("/api/v1")
public class NotificationController {

    private static final Logger log = LoggerFactory.getLogger(NotificationController.class);

    private final NotificationRepository notifications;
    private final DeliveryAttemptRepository attempts;
    private final SupplierRegistry registry;
    private final Metrics metrics;
    private final TokenAuth auth;

    public NotificationController(NotificationRepository notifications,
                                  DeliveryAttemptRepository attempts,
                                  SupplierRegistry registry,
                                  Metrics metrics,
                                  TokenAuth auth) {
        this.notifications = notifications;
        this.attempts = attempts;
        this.registry = registry;
        this.metrics = metrics;
        this.auth = auth;
    }

    /** 提交通知请求 */
    @PostMapping("/notifications")
    public ResponseEntity<?> submit(@RequestHeader(value = "Authorization", required = false) String authorization,
                                    @Valid @RequestBody SubmitRequest req) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("error", "invalid or missing bearer token"));
        }
        if (registry.get(req.supplier()) == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", "unknown supplier",
                    "available", registry.names()));
        }
        if (req.payload() == null || req.payload().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "payload required (JSON)"));
        }

        Notification n = new Notification();
        n.setId(UUID.randomUUID().toString());
        n.setSource(auth.currentSource());
        n.setSupplier(req.supplier());
        n.setEventType(req.eventType());
        n.setPayload(req.payload());
        n.setDedupKey(req.dedupKey());
        n.setStatus(Notification.Status.PENDING);
        n.setAttempt(0);
        n.setMaxAttempts(8);
        n.setNextAttemptAt(Instant.now());
        n.setCreatedAt(Instant.now());
        n.setUpdatedAt(Instant.now());

        boolean inserted = notifications.insert(n);
        metrics.deliveryCounter("accepted");
        if (!inserted) {
            // 唯一索引冲突 = 同 source+dedup_key 重复提交 → 幂等返回已有记录
            Notification existing = notifications.findByDedup(n.getSource(), req.dedupKey()).orElse(null);
            if (existing != null) {
                return ResponseEntity.status(HttpStatus.OK).body(Map.of(
                        "notification_id", existing.getId(),
                        "status", existing.getStatus().name(),
                        "deduplicated", true));
            }
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", "insert conflict"));
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "notification_id", n.getId(),
                "status", n.getStatus().name()));
    }

    /** 查询通知状态（业务方可轮询，或后续扩展 webhook 回调） */
    @GetMapping("/notifications/{id}")
    public ResponseEntity<?> get(@RequestHeader(value = "Authorization", required = false) String authorization,
                                 @PathVariable String id) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "unauthorized"));
        }
        return notifications.findById(id)
                .<ResponseEntity<?>>map(n -> ResponseEntity.ok(toView(n)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", "not found")));
    }

    /** 投递轨迹：这条通知的每次出站尝试 */
    @GetMapping("/notifications/{id}/attempts")
    public ResponseEntity<?> attempts(@RequestHeader(value = "Authorization", required = false) String authorization,
                                      @PathVariable String id) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "unauthorized"));
        }
        return ResponseEntity.ok(attempts.findByNotification(id));
    }

    /** 死信列表 + 单条重发（对应"绝不静默丢弃"的运营兜底） */
    @GetMapping("/dead-letters")
    public ResponseEntity<?> deadLetters(@RequestHeader(value = "Authorization", required = false) String authorization,
                                         @RequestParam(defaultValue = "20") int limit) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "unauthorized"));
        }
        List<Map<String, Object>> view = notifications.findDead(limit).stream().map(this::toView).toList();
        return ResponseEntity.ok(view);
    }

    @PostMapping("/dead-letters/{id}/redeliver")
    public ResponseEntity<?> redeliver(@RequestHeader(value = "Authorization", required = false) String authorization,
                                       @PathVariable String id) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "unauthorized"));
        }
        int rows = notifications.redeliver(id);
        if (rows == 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "not found or not in DEAD status"));
        }
        return ResponseEntity.ok(Map.of("notification_id", id, "status", "PENDING"));
    }

    /** 供应商列表（接入自助查询） */
    @GetMapping("/suppliers")
    public ResponseEntity<?> suppliers(@RequestHeader(value = "Authorization", required = false) String authorization) {
        if (!auth.check(authorization)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("error", "unauthorized"));
        }
        return ResponseEntity.ok(registry.names());
    }

    /** 运行指标 */
    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        return metrics.snapshot();
    }

    private Map<String, Object> toView(Notification n) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("notification_id", n.getId());
        m.put("source", n.getSource());
        m.put("supplier", n.getSupplier());
        m.put("event_type", n.getEventType());
        m.put("status", n.getStatus().name());
        m.put("attempt", n.getAttempt());
        m.put("max_attempts", n.getMaxAttempts());
        m.put("next_attempt_at", n.getNextAttemptAt() == null ? null : n.getNextAttemptAt().toString());
        m.put("delivered_at", n.getDeliveredAt() == null ? null : n.getDeliveredAt().toString());
        m.put("last_error", n.getLastError());
        m.put("created_at", n.getCreatedAt().toString());
        return m;
    }

    public record SubmitRequest(String supplier, String eventType, String payload, String dedupKey) {}
}
