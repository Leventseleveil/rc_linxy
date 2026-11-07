package com.linxy.notify;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.TestPropertySource;

import com.linxy.notify.domain.Notification;
import com.linxy.notify.infra.NotificationRepository;
import com.linxy.notify.supplier.SupplierSpec;

/**
 * 端到端验收：内嵌 HttpServer 模拟外部供应商（200 / 先500后200 / 400），
 * 验证 提交 → 投递 → 成功 / 重试 → 死信 / 幂等 / 鉴权 全链路。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.datasource.url=jdbc:sqlite:./target/e2e.db",
        "notify.suppliers-dir=./suppliers-e2e",
        "notify.delivery.poll-interval-ms=300",
        "notify.retry.backoff-seconds=0,0,1",
        "notify.security.api-tokens=test-token"
})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class EndToEndTest {

    @LocalServerPort int port;
    @Autowired org.springframework.boot.test.web.client.TestRestTemplate rest;
    @Autowired NotificationRepository notifications;

    static HttpServer vendor;
    static int vendorPort;
    static final AtomicInteger flakyHits = new AtomicInteger();
    static final Map<String, Integer> hitsByPath = new ConcurrentHashMap<>();

    @BeforeAll
    static void startVendor() throws Exception {
        vendor = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        vendorPort = vendor.getAddress().getPort();

        vendor.createContext("/ok", ex -> reply(ex, 200, "{\"ok\":true}"));
        vendor.createContext("/flaky", ex -> {
            // 第一次 500，之后 200 —— 演示"可重试失败 → 退避 → 成功"
            if (flakyHits.incrementAndGet() == 1) reply(ex, 500, "{\"err\":\"boom\"}");
            else reply(ex, 200, "{\"ok\":true}");
        });
        vendor.createContext("/reject", ex -> reply(ex, 400, "{\"err\":\"bad\"}"));

        vendor.start();

        // 写 E2E 供应商配置，指向内嵌 vendor 端口
        Path dir = Path.of("./suppliers-e2e");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("e2e_ok.yaml"), """
                name: e2e_ok
                request:
                  method: POST
                  url: http://127.0.0.1:%d/ok
                  headers:
                    Content-Type: application/json
                  body: '{"eventType": "{{event_type}}", "userId": "{{payload.user_id}}"}'
                """.formatted(vendorPort));
        Files.writeString(dir.resolve("e2e_flaky.yaml"), """
                name: e2e_flaky
                request:
                  method: POST
                  url: http://127.0.0.1:%d/flaky
                  headers:
                    Content-Type: application/json
                  body: '{"eventType": "{{event_type}}"}'
                retry:
                  backoff_seconds: [0, 0, 1]
                """.formatted(vendorPort));
        Files.writeString(dir.resolve("e2e_reject.yaml"), """
                name: e2e_reject
                request:
                  method: POST
                  url: http://127.0.0.1:%d/reject
                  headers:
                    Content-Type: application/json
                  body: '{"eventType": "{{event_type}}"}'
                retry:
                  backoff_seconds: [0, 0, 1]
                """.formatted(vendorPort));
    }

    @AfterAll
    static void stopVendor() {
        if (vendor != null) vendor.stop(0);
    }

    static void reply(com.sun.net.httpserver.HttpExchange ex, int code, String body) {
        try {
            hitsByPath.merge(ex.getRequestURI().getPath(), 1, Integer::sum);
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(code, b.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(b); }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // ---------- 测试 ----------

    @Test
    @Order(1)
    void submitDelivers() {
        var body = Map.of("supplier", "e2e_ok", "eventType", "user.registered",
                "payload", "{\"user_id\":\"u-42\"}", "dedupKey", "e2e-1-" + UUID.randomUUID());
        var resp = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        String id = (String) resp.getBody().get("notification_id");

        await(() -> notifications.findById(id)
                .map(n -> n.getStatus() == Notification.Status.DELIVERED)
                .orElse(false), 8000);
    }

    @Test
    @Order(2)
    void flakyVendorRetriesToSuccess() {
        var body = Map.of("supplier", "e2e_flaky", "eventType", "payment.succeeded",
                "payload", "{\"order_id\":\"o-1\"}", "dedupKey", "e2e-2-" + UUID.randomUUID());
        var resp = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(202);
        String id = (String) resp.getBody().get("notification_id");

        await(() -> notifications.findById(id)
                .map(n -> n.getStatus() == Notification.Status.DELIVERED && n.getAttempt() >= 2)
                .orElse(false), 10000);
    }

    @Test
    @Order(3)
    void nonRetryableGoesDeadImmediately() {
        var body = Map.of("supplier", "e2e_reject", "eventType", "stock.change",
                "payload", "{\"sku\":\"s-1\"}", "dedupKey", "e2e-3");
        var resp = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        String id = (String) resp.getBody().get("notification_id");

        await(() -> notifications.findById(id)
                .map(n -> n.getStatus() == Notification.Status.DEAD)
                .orElse(false), 8000);
        // 400 只投一次，不应重试
        Notification n = notifications.findById(id).orElseThrow();
        assertThat(n.getAttempt()).isEqualTo(1);
    }

    @Test
    @Order(4)
    void dedupKeyIdempotent() {
        var body = Map.of("supplier", "e2e_ok", "eventType", "user.registered",
                "payload", "{\"user_id\":\"u-43\"}", "dedupKey", "e2e-dup-" + UUID.randomUUID());
        var r1 = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        var r2 = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        assertThat(r1.getStatusCode().value()).isEqualTo(202);
        assertThat(r2.getStatusCode().value()).isEqualTo(200);
        assertThat(r2.getBody()).containsEntry("deduplicated", true);
        assertThat(r2.getBody().get("notification_id")).isEqualTo(r1.getBody().get("notification_id"));
    }

    @Test
    @Order(5)
    void unknownSupplierRejected() {
        var body = Map.of("supplier", "nope", "eventType", "x", "payload", "{}");
        var resp = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        assertThat(resp.getStatusCode().value()).isEqualTo(400);
    }

    @Test
    @Order(6)
    void unauthorizedRejected() throws Exception {
        // 用原生 HttpClient：TestRestTemplate 对 401 的认证重试行为会干扰断言
        var client = java.net.http.HttpClient.newHttpClient();
        var req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create("http://localhost:" + port + "/api/v1/notifications"))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(
                        "{\"supplier\":\"e2e_ok\",\"eventType\":\"x\",\"payload\":\"{}\"}"))
                .build();
        var resp = client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(401);
    }

    @Test
    @Order(7)
    void supplierSpecParses() throws Exception {
        Path tmp = Files.createTempFile("spec", ".yaml");
        Files.writeString(tmp, """
                name: t1
                request:
                  method: POST
                  url: https://a.example.com/x
                  headers:
                    Authorization: "Bearer {{secret.k}}"
                  body: |
                    {"a": "{{payload.a}}"}
                retry:
                  backoff_seconds: [1, 2, 3]
                rate_limit:
                  qps: 10
                  burst: 20
                """);
        SupplierSpec spec = SupplierSpec.load(tmp);
        assertThat(spec.name()).isEqualTo("t1");
        assertThat(spec.method()).isEqualTo("POST");
        assertThat(spec.url()).isEqualTo("https://a.example.com/x");
        assertThat(spec.headers()).containsEntry("Authorization", "Bearer {{secret.k}}");
        assertThat(spec.backoffSeconds()).containsExactly(1, 2, 3);
        assertThat(spec.qps()).isEqualTo(10);
        assertThat(spec.burst()).isEqualTo(20);
    }

    @Test
    @Order(8)
    void deadLetterRedeliverWorks() throws Exception {
        // 先制造一条死信（复用 e2e_reject），再人工重发，验证回到 PENDING
        var body = Map.of("supplier", "e2e_reject", "eventType", "stock.change",
                "payload", "{\"sku\":\"s-2\"}", "dedupKey", "e2e-dlq-" + UUID.randomUUID());
        var resp = rest.postForEntity("/api/v1/notifications", request(body), Map.class);
        String id = (String) resp.getBody().get("notification_id");
        await(() -> notifications.findById(id)
                .map(n -> n.getStatus() == Notification.Status.DEAD)
                .orElse(false), 8000);

        var dlqResp = rest.exchange("/api/v1/dead-letters",
                org.springframework.http.HttpMethod.GET,
                new org.springframework.http.HttpEntity<>(withAuth()), List.class);
        assertThat(dlqResp.getStatusCode().value()).isEqualTo(200);

        var redeliverResp = rest.postForEntity("/api/v1/dead-letters/" + id + "/redeliver",
                new org.springframework.http.HttpEntity<>(withAuth()), Map.class);
        assertThat(redeliverResp.getStatusCode().value()).isEqualTo(200);
        Notification n = notifications.findById(id).orElseThrow();
        assertThat(n.getStatus()).isEqualTo(Notification.Status.PENDING);
        assertThat(n.getAttempt()).isZero();
    }

    // ---------- helpers ----------

    private <T extends Map<String, ?>> org.springframework.http.HttpEntity<T> request(T body) {
        return new org.springframework.http.HttpEntity<>(body, withAuth());
    }

    private org.springframework.http.HttpHeaders withAuth() {
        var h = new org.springframework.http.HttpHeaders();
        h.set("Authorization", "Bearer test-token");
        return h;
    }

    private static void await(java.util.function.BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            try { Thread.sleep(100); } catch (InterruptedException e) { return; }
        }
        throw new AssertionError("condition not met within " + timeoutMs + "ms");
    }
}
