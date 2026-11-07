package com.linxy.notify.supplier;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * 供应商规格：一个外部系统 = 一份声明式 YAML。
 *
 * 设计取舍：用"声明式模板"而不是"每供应商一个 Java Client 类"（mockingbird 的做法），
 * 因为通知类 API 的差异仅在于 URL/Header/Body 的形状，模板即可覆盖；
 * 新供应商零代码上线。复杂签名类供应商可通过 custom 渲染器扩展（见 Renderer）。
 *
 * @param name              供应商标识（业务方提交时引用）
 * @param method            HTTP 方法
 * @param url               URL，支持 {{var}} 模板
 * @param headers           Header 模板
 * @param bodyTemplate      Body 模板
 * @param backoffSeconds    该供应商专属退避序列（秒）；空则用全局默认
 * @param qps               令牌桶稳态 QPS
 * @param burst             令牌桶桶容量
 * @param connectTimeoutMs  连接超时
 * @param readTimeoutMs     读超时
 */
public record SupplierSpec(
        String name,
        String method,
        String url,
        Map<String, String> headers,
        String bodyTemplate,
        List<Integer> backoffSeconds,
        Integer qps,
        Integer burst,
        Integer connectTimeoutMs,
        Integer readTimeoutMs) {

    /** 从 YAML 文件解析（简易解析：不引 SnakeYAML，读约定格式；见 SpecLoader） */
    public static SupplierSpec load(Path p) throws Exception {
        return SpecLoader.load(p);
    }
}
