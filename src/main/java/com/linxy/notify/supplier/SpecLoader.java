package com.linxy.notify.supplier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 供应商 YAML 的极简解析器。
 *
 * 为什么手写而不用 SnakeYAML：供应商文件格式是受控的浅层结构（两层），
 * 手写解析器 100 来行即可覆盖，避免为演示项目引入额外依赖树；
 * 且能在解析阶段就做校验（缺 url / method 直接抛错），错误信息更友好。
 * 生产若需完整 YAML 能力，换 SnakeYAML 只是替换单个类。
 */
public final class SpecLoader {

    private SpecLoader() {}

    private static final Pattern KEY = Pattern.compile("^(\\s*)([A-Za-z_][\\w-]*):\\s*(.*)$");
    private static final Pattern LIST_ITEM = Pattern.compile("^\\s*-\\s+(.*)$");

    public static SupplierSpec load(Path p) throws IOException {
        List<String> lines = Files.readAllLines(p, StandardCharsets.UTF_8);
        Map<String, Object> root = parseBlock(lines, 0, 0).map();
        @SuppressWarnings("unchecked")
        Map<String, Object> req = (Map<String, Object>) root.getOrDefault("request", Map.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> retry = (Map<String, Object>) root.getOrDefault("retry", Map.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> rl = (Map<String, Object>) root.getOrDefault("rate_limit", Map.of());
        @SuppressWarnings("unchecked")
        Map<String, Object> to = (Map<String, Object>) root.getOrDefault("timeout", Map.of());

        String name = str(root, "name");
        String method = str(req, "method");
        String url = str(req, "url");
        if (name == null || method == null || url == null) {
            throw new IllegalArgumentException("Supplier spec missing required field: " + p);
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> headersRaw = (Map<String, Object>) req.getOrDefault("headers", Map.of());
        Map<String, String> headers = new LinkedHashMap<>();
        headersRaw.forEach((k, v) -> headers.put(k, String.valueOf(v)));

        List<Integer> backoff = null;
        Object bo = retry.get("backoff_seconds");
        if (bo instanceof List<?> l) {
            backoff = l.stream().map(x -> Integer.parseInt(String.valueOf(x).trim())).toList();
        }

        return new SupplierSpec(
                name, method, url, headers,
                (String) req.get("body"),
                backoff,
                intOrNull(rl, "qps"), intOrNull(rl, "burst"),
                intOrNull(to, "connect_ms"), intOrNull(to, "read_ms"));
    }

    // ---- 极简缩进式 YAML 子集解析 ----

    private record ParseResult(Map<String, Object> map, int nextLine) {}

    private static ParseResult parseBlock(List<String> lines, int start, int indent) {
        Map<String, Object> out = new LinkedHashMap<>();
        int i = start;
        while (i < lines.size()) {
            String line = lines.get(i);
            String stripped = line.strip();
            if (stripped.isEmpty() || stripped.startsWith("#")) { i++; continue; }

            int curIndent = line.length() - line.stripLeading().length();
            if (curIndent < indent) break;
            if (curIndent > indent) {
                throw new IllegalArgumentException("Unexpected indent at line " + (i + 1) + ": " + line);
            }

            Matcher m = KEY.matcher(stripped);
            if (m.matches()) {
                String key = m.group(2);
                String val = m.group(3).trim();
                if (val.isEmpty()) {
                    // 值为空：可能是嵌套 block 或 block scalar
                    if (i + 1 < lines.size()) {
                        String next = lines.get(i + 1);
                        int nextIndent = next.length() - next.stripLeading().length();
                        String nextStripped = next.strip();
                        if (nextIndent > curIndent && nextStripped.startsWith("|")) {
                            // 不应发生：block scalar 标记在 key 行
                        }
                    }
                    // 尝试解析嵌套 block 或列表
                    if (i + 1 < lines.size()) {
                        String next = lines.get(i + 1);
                        int nextIndent = next.length() - next.stripLeading().length();
                        String nextStripped = next.strip();
                        if (!nextStripped.isEmpty() && nextIndent > curIndent) {
                            if (LIST_ITEM.matcher(nextStripped).matches()) {
                                List<String> items = new ArrayList<>();
                                int j = i + 1;
                                while (j < lines.size()) {
                                    String nl = lines.get(j).strip();
                                    int ni = lines.get(j).length() - lines.get(j).stripLeading().length();
                                    if (ni == nextIndent && LIST_ITEM.matcher(nl).matches()) {
                                        items.add(LIST_ITEM.matcher(nl).group(1).trim());
                                        j++;
                                    } else break;
                                }
                                out.put(key, items);
                                i = j;
                                continue;
                            } else {
                                ParseResult sub = parseBlock(lines, i + 1, nextIndent);
                                out.put(key, sub.map());
                                i = sub.nextLine();
                                continue;
                            }
                        }
                    }
                    out.put(key, new LinkedHashMap<String, Object>());
                    i++;
                } else if (val.equals("|")) {
                    // block scalar：取后续更深缩进行
                    StringBuilder sb = new StringBuilder();
                    int j = i + 1;
                    int blockIndent = -1;
                    while (j < lines.size()) {
                        String nl = lines.get(j);
                        if (nl.strip().isEmpty()) { sb.append('\n'); j++; continue; }
                        int ni = nl.length() - nl.stripLeading().length();
                        if (blockIndent == -1) blockIndent = ni;
                        if (ni < blockIndent) break;
                        sb.append(nl.substring(blockIndent)).append('\n');
                        j++;
                    }
                    out.put(key, sb.toString().stripTrailing());
                    i = j;
                } else if (val.startsWith("[")) {
                    // flow 序列：[a, b, c]
                    String inner = val.substring(1, val.endsWith("]") ? val.length() - 1 : val.length());
                    List<String> items = new ArrayList<>();
                    for (String item : inner.split(",")) {
                        if (!item.isBlank()) items.add(unquote(item.trim()));
                    }
                    out.put(key, items);
                    i++;
                } else if (val.startsWith("{")) {
                    // flow 映射：{k1: v1, k2: v2}
                    String inner = val.substring(1, val.endsWith("}") ? val.length() - 1 : val.length());
                    Map<String, Object> flowMap = new LinkedHashMap<>();
                    for (String pair : inner.split(",")) {
                        if (pair.isBlank()) continue;
                        String[] kv = pair.split(":", 2);
                        if (kv.length == 2) flowMap.put(unquote(kv[0].trim()), unquote(kv[1].trim()));
                    }
                    out.put(key, flowMap);
                    i++;
                } else {
                    out.put(key, unquote(val));
                    i++;
                }
            } else if (LIST_ITEM.matcher(stripped).matches()) {
                throw new IllegalArgumentException("Unexpected list item at line " + (i + 1));
            } else {
                throw new IllegalArgumentException("Cannot parse line " + (i + 1) + ": " + line);
            }
        }
        return new ParseResult(out, i);
    }

    private static String unquote(String v) {
        String s = v.trim();
        if (s.length() >= 2 && ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'")))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    private static String str(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : String.valueOf(v);
    }

    private static Integer intOrNull(Map<String, Object> m, String k) {
        Object v = m.get(k);
        return v == null ? null : Integer.parseInt(String.valueOf(v).trim());
    }
}
