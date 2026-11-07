package com.linxy.notify.delivery;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 出站 HTTP 执行器（对应 mockingbird 的 BaseHttpSinglePushClient.doHttpCall）。
 * 供应商维度隔离超时参数；每次调用记入审计。
 */
@Component
public class HttpDeliveryClient {

    private static final Logger log = LoggerFactory.getLogger(HttpDeliveryClient.class);

    private final Duration defaultConnect;
    private final Duration defaultRead;

    public HttpDeliveryClient(@Value("${notify.delivery.connect-timeout-ms:3000}") int connectMs,
                              @Value("${notify.delivery.read-timeout-ms:5000}") int readMs) {
        this.defaultConnect = Duration.ofMillis(connectMs);
        this.defaultRead = Duration.ofMillis(readMs);
    }

    /** 对外投递一次。任何异常都不抛出 —— 全部折叠为 Result。 */
    public DeliveryResult deliver(SupplierRequest req) {
        long start = System.nanoTime();
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(req.connectTimeout() == null ? defaultConnect : Duration.ofMillis(req.connectTimeout()))
                    .build();

            HttpRequest.Builder hb = HttpRequest.newBuilder()
                    .uri(URI.create(req.url()))
                    .timeout(req.readTimeout() == null ? defaultRead : Duration.ofMillis(req.readTimeout()));

            req.headers().forEach(hb::header);
            // 幂等去重头：让接收方能识别重试造成的重复请求（对应 APNs collapseId 思想）
            if (req.dedupHeaderValue() != null) {
                hb.header(req.dedupHeaderName(), req.dedupHeaderValue());
            }

            HttpRequest httpReq = switch (req.method().toUpperCase()) {
                case "GET" -> hb.GET().build();
                case "POST" -> hb.POST(HttpRequest.BodyPublishers.ofString(req.body())).build();
                case "PUT" -> hb.PUT(HttpRequest.BodyPublishers.ofString(req.body())).build();
                default -> throw new IllegalArgumentException("unsupported method: " + req.method());
            };

            HttpResponse<String> resp = client.send(httpReq, HttpResponse.BodyHandlers.ofString());
            long costMs = (System.nanoTime() - start) / 1_000_000;
            Outcome.Result classified = Outcome.classify(resp.statusCode());
            return new DeliveryResult(classified, costMs, resp.statusCode(), resp.body());

        } catch (HttpTimeoutException e) {
            long costMs = (System.nanoTime() - start) / 1_000_000;
            // 超时 = 中间状态：对方可能已收到。选择重试（at-least-once），靠去重头让对方幂等。
            return new DeliveryResult(Outcome.Result.transport("read timeout"), costMs, null, e.getMessage());
        } catch (java.net.ConnectException e) {
            long costMs = (System.nanoTime() - start) / 1_000_000;
            return new DeliveryResult(Outcome.Result.transport("connect refused: " + e.getMessage()), costMs, null, null);
        } catch (Exception e) {
            long costMs = (System.nanoTime() - start) / 1_000_000;
            log.error("deliver error url={}", req.url(), e);
            return new DeliveryResult(Outcome.Result.transport("error: " + e.getClass().getSimpleName()), costMs, null, e.getMessage());
        }
    }

    public record SupplierRequest(String method, String url, Map<String, String> headers, String body,
                                  String dedupHeaderName, String dedupHeaderValue,
                                  Integer connectTimeout, Integer readTimeout) {}

    public record DeliveryResult(Outcome.Result outcome, long durationMs, Integer rawStatus, String responseBody) {}
}
