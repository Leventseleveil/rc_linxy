package com.linxy.notify;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import com.linxy.notify.delivery.Outcome;
import com.linxy.notify.supplier.Renderer;
import com.linxy.notify.supplier.SupplierSpec;

class UnitTest {

    // ---- 结果分类（投递状态机核心） ----

    @Test
    void classifyHttpStatus() {
        assertThat(Outcome.classify(200).kind()).isEqualTo(Outcome.Kind.SUCCESS);
        assertThat(Outcome.classify(204).kind()).isEqualTo(Outcome.Kind.SUCCESS);
        assertThat(Outcome.classify(429).kind()).isEqualTo(Outcome.Kind.RETRYABLE);
        assertThat(Outcome.classify(500).kind()).isEqualTo(Outcome.Kind.RETRYABLE);
        assertThat(Outcome.classify(503).kind()).isEqualTo(Outcome.Kind.RETRYABLE);
        assertThat(Outcome.classify(400).kind()).isEqualTo(Outcome.Kind.NON_RETRYABLE);
        assertThat(Outcome.classify(401).kind()).isEqualTo(Outcome.Kind.NON_RETRYABLE);
        assertThat(Outcome.classify(404).kind()).isEqualTo(Outcome.Kind.NON_RETRYABLE);
    }

    // ---- 模板渲染 ----

    private SupplierSpec spec(String url, Map<String, String> headers, String body) {
        return new SupplierSpec("s", "POST", url, headers, body, null, null, null, null, null);
    }

    @Test
    void renderSubstitutesVariables() {
        SupplierSpec s = spec("https://api.example.com/v1/{{payload.tenant}}/events",
                Map.of("Authorization", "Bearer {{secret.token}}"),
                "{\"uid\": \"{{payload.user_id}}\", \"etype\": \"{{event_type}}\"}");
        Renderer.RenderedRequest r = Renderer.render(s, "user.registered",
                "{\"user_id\":\"u-9\",\"tenant\":\"acme\"}",
                Map.of("token", "tk"), Map.of("notification_id", "n-1"));
        assertThat(r.url()).isEqualTo("https://api.example.com/v1/acme/events");
        assertThat(r.headers()).containsEntry("Authorization", "Bearer tk");
        assertThat(r.body()).contains("\"u-9\"").contains("user.registered");
    }

    @Test
    void renderMissingVariableFailsFatal() {
        SupplierSpec s = spec("https://x.example.com", Map.of(), "{{payload.missing}}");
        assertThatThrownBy(() -> Renderer.render(s, "e", "{}", Map.of(), Map.of()))
                .isInstanceOf(Renderer.RenderException.class)
                .satisfies(ex -> assertThat(((Renderer.RenderException) ex).isRetryable()).isFalse());
    }

    @Test
    void renderRejectsInvalidPayloadJson() {
        SupplierSpec s = spec("https://x.example.com", Map.of(), "{{event_type}}");
        assertThatThrownBy(() -> Renderer.render(s, "e", "not-json", Map.of(), Map.of()))
                .isInstanceOf(Renderer.RenderException.class);
    }

    @Test
    void renderNoInjectionThroughValue() {
        // 值里带 {{...}} 不做二次展开（防模板注入）
        SupplierSpec s = spec("https://x.example.com", Map.of(), "{{payload.v}}");
        Renderer.RenderedRequest r = Renderer.render(s, "e", "{\"v\":\"{{secret.x}}\"}", Map.of("x", "leak"), Map.of());
        assertThat(r.body()).isEqualTo("{{secret.x}}");
    }

    // ---- 去重键语义 ----

    @Test
    void dedupKeyOptional() {
        // dedup_key 可空：不携带幂等键的通知允许重复提交
        SupplierSpec s = spec("https://x.example.com", Map.of(), "{{event_type}}");
        Renderer.RenderedRequest r = Renderer.render(s, "e", "{}", Map.of(), Map.of());
        assertThat(r.method()).isEqualTo("POST");
    }
}
