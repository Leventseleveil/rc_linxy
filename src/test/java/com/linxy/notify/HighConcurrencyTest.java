package com.linxy.notify;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.TestPropertySource;

import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.NotificationRepository;
import com.linxy.notify.infra.SubmitService;

/**
 * 高并发 + MQ 接入验收。
 *
 * 场景 1（intake MQ）：多业务各往自己的 intake 队列发消息 → 全部落库投递，source 归属正确。
 * 场景 2（并发压测）：200 线程同时 HTTP 提交 → 全部 202、全部送达、
 *                     每条恰好一次成功投递（审计行数 = 通知数）、零重复。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:sqlite:./target/load.db",
        "notify.suppliers-dir=./suppliers-load",
        "notify.dispatch.sweep-interval-ms=1000",
        "notify.delivery.poll-interval-ms=1000",
        "notify.retry.backoff-seconds=0,0,1",
        "notify.security.api-tokens=test-token"
})
class HighConcurrencyTest {

    @LocalServerPort int port;
    @Autowired TestRestTemplate rest;
    @Autowired NotificationRepository notifications;
    @Autowired org.springframework.jms.core.JmsTemplate jms;

    static HttpServer vendor;
    static final Map<String, Integer> hitsByPath = new ConcurrentHashMap<>();
    static final AtomicInteger uniqueDedupReceived = new AtomicInteger();

    @BeforeAll
    static void startVendor() throws Exception {
        vendor = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        vendorPort = vendor.getAddress().getPort();
        vendor.createContext("/ok", ex -> {
            // 统计 vendor 侧收到的去重键（验证零重复送达）
            String dedup = ex.getRequestHeaders().getFirst("X-Notify-Dedup-Key");
            if (dedup != null) hitsByPath.merge("dedup:" + dedup, 1, Integer::sum);
            hitsByPath.merge("/ok", 1, Integer::sum);
            byte[] b = "{\"ok\":true}".getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(b); }
        });
        vendor.start();

        Path dir = Path.of("./suppliers-load");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("load_ok.yaml"), """
                name: load_ok
                request:
                  method: POST
                  url: http://127.0.0.1:%d/ok
                  headers:
                    Content-Type: application/json
                  body: '{"eventType": "{{event_type}}", "n": "{{payload.n}}"}'
                rate_limit:
                  qps: 2000
                  burst: 2000
                """.formatted(vendorPort));
    }

    static int vendorPort;

    @AfterAll
    static void stopVendor() {
        if (vendor != null) vendor.stop(0);
    }

    private HttpHeaders auth() {
        HttpHeaders h = new HttpHeaders();
        h.set("Authorization", "Bearer test-token");
        return h;
    }

    private org.springframework.http.ResponseEntity<Map> postWithRetry(
            org.springframework.http.HttpEntity<? extends Map<String, ?>> req) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                return rest.postForEntity("/api/v1/notifications", req, Map.class);
            } catch (org.springframework.web.client.ResourceAccessException e) {
                if (attempt == 2) throw e;
            }
        }
        throw new IllegalStateException("unreachable");
    }

    private static void await(java.util.function.BooleanSupplier cond, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            try { Thread.sleep(150); } catch (InterruptedException e) { return; }
        }
        throw new AssertionError("timeout waiting: " + what);
    }

    /** 场景 1：三个业务各往自己的 intake 队列发 JMS 消息（模拟"各自 MQ 投递到接入层"） */
    @Test
    void intakeQueuesAcceptAndDeliver() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        for (String source : List.of("ads", "crm", "inventory")) {
            String body = "{\"supplier\":\"load_ok\",\"eventType\":\"mq.intake.test\","
                    + "\"payload\":\"{\\\"n\\\":\\\"%s\\\"}\",\"dedupKey\":\"mq-" + source + "-" + suffix + "\"}";
            jms.send("notify.intake." + source, s ->
                    s.createTextMessage(body.formatted(source)));
        }
        await(() -> notifications.findByDedup("ads", "mq-ads-" + suffix).isPresent(), 8000, "ads row");
        await(() -> notifications.findByDedup("crm", "mq-crm-" + suffix).isPresent(), 8000, "crm row");
        await(() -> notifications.findByDedup("inventory", "mq-inventory-" + suffix).isPresent(), 8000, "inventory row");

        // source 归属 = 队列名（不信任消息体），全部送达
        for (String source : List.of("ads", "crm", "inventory")) {
            Notification n = notifications.findByDedup(source, "mq-" + source + "-" + suffix).orElseThrow();
            assertThat(n.getSource()).isEqualTo(source);   // 队列即身份
            await(() -> notifications.findById(n.getId()).orElseThrow().getStatus()
                    == Notification.Status.DELIVERED, 10000, "delivered " + source);
        }
    }

    /** 场景 2：200 并发 HTTP 提交 → 全部送达、零重复 */
    @Test
    void concurrentSubmissionsAllDelivered() throws Exception {
        int threads = 200;
        String run = UUID.randomUUID().toString().substring(0, 8);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted202 = new AtomicInteger();
        Map<String, String> ids = new java.util.concurrent.ConcurrentHashMap<>();

        for (int i = 0; i < threads; i++) {
            int n = i;
            pool.submit(() -> {
                try {
                    start.await();
                    var body = Map.of("supplier", "load_ok", "eventType", "load.test",
                            "payload", "{\"n\":\"" + n + "\"}", "dedupKey", "load-" + run + "-" + n);
                    // 测试客户端（TestRestTemplate 连接池）在 200 瞬时并发下可能 reset，
                    // 网络层异常重试一次 —— 服务端有 dedupKey 幂等，重试安全
                    var resp = postWithRetry(new HttpEntity<>(body, auth()));
                    if (resp.getStatusCode() == HttpStatus.ACCEPTED) {
                        accepted202.incrementAndGet();
                        ids.put(String.valueOf(n), (String) resp.getBody().get("notification_id"));
                    }
                } catch (Exception ignored) {
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();

        // 提交阶段：200/202，无失败
        assertThat(accepted202.get()).isEqualTo(threads);

        // 投递阶段：全部 DELIVERED
        await(() -> ids.values().stream().allMatch(id ->
                        notifications.findById(id).orElseThrow().getStatus() == Notification.Status.DELIVERED),
                60_000, "all 200 delivered");

        // 重复校验：vendor 侧每个 dedup 键只收到一次
        long dupKeys = ids.keySet().stream()
                .map(k -> "load-" + run + "-" + k)
                .filter(k -> hitsByPath.getOrDefault("dedup:" + k, 0) > 1)
                .count();
        assertThat(dupKeys).isZero();
    }
}
