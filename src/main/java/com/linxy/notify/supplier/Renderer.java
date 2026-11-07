package com.linxy.notify.supplier;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 模板渲染器：把 {{payload.xxx}} / {{event_type}} / {{secret.xxx}} / {{meta.*}}
 * 替换为实际值，生成最终出站 URL / Header / Body。
 *
 * 安全性：值不做二次模板展开（防注入）；渲染失败（变量缺失）视为
 * NON_RETRYABLE —— 参数问题重试也不会好，尽早暴露给业务方。
 */
public final class Renderer {

    private Renderer() {}

    private static final Pattern VAR = Pattern.compile("\\{\\{\\s*([\\w.]+)\\s*}}");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public record RenderedRequest(String method, String url, Map<String, String> headers, String body) {}

    /**
     * @param spec       供应商定义
     * @param eventType  事件类型
     * @param payloadJson 业务事件 JSON
     * @param secrets    供应商密钥（从环境变量/密钥管理注入，不落库）
     * @param meta       元信息：notification_id / dedup_key / attempt 等
     */
    public static RenderedRequest render(SupplierSpec spec, String eventType, String payloadJson,
                                         Map<String, String> secrets, Map<String, String> meta) {
        JsonNode payload;
        try {
            payload = MAPPER.readTree(payloadJson == null || payloadJson.isBlank() ? "{}" : payloadJson);
        } catch (Exception e) {
            throw new RenderException("payload is not valid JSON: " + e.getMessage(), false);
        }

        Map<String, String> vars = new HashMap<>();
        vars.put("event_type", eventType);
        vars.put("notification_id", meta.getOrDefault("notification_id", ""));
        vars.put("dedup_key", meta.getOrDefault("dedup_key", ""));
        vars.put("attempt", meta.getOrDefault("attempt", "1"));
        vars.put("timestamp", String.valueOf(Instant.now().getEpochSecond()));
        payload.fieldNames().forEachRemaining(fn -> {
            JsonNode v = payload.get(fn);
            vars.put("payload." + fn, v.isValueNode() ? v.asText() : v.toString());
        });
        secrets.forEach((k, v) -> vars.put("secret." + k, v));

        String url = renderString(spec.url(), vars);
        Map<String, String> headers = new java.util.LinkedHashMap<>();
        spec.headers().forEach((k, v) -> headers.put(k, renderString(v, vars)));
        String body = spec.bodyTemplate() == null ? "" : renderString(spec.bodyTemplate(), vars);
        return new RenderedRequest(spec.method(), url, headers, body);
    }

    static String renderString(String template, Map<String, String> vars) {
        Matcher m = VAR.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String val = vars.get(key);
            if (val == null) {
                throw new RenderException("missing template variable: " + key, false);
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(val));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** 渲染异常；retryable=false 表示重试无意义 */
    public static class RenderException extends RuntimeException {
        private final boolean retryable;
        public RenderException(String msg, boolean retryable) {
            super(msg);
            this.retryable = retryable;
        }
        public boolean isRetryable() { return retryable; }
    }
}
