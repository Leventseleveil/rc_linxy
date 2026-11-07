package com.linxy.notify.infra;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.linxy.notify.supplier.SupplierSpec;

import jakarta.annotation.PostConstruct;

/**
 * 供应商注册表：启动时扫描 suppliers-dir 加载全部供应商定义，支持热加载（每 30s 重扫一次）。
 * 新增一个外部系统 = 新增一个 yaml 文件，无需改代码发版 —— 这是"多供应商不同格式"的核心解法。
 */
@Component
public class SupplierRegistry {

    private static final Logger log = LoggerFactory.getLogger(SupplierRegistry.class);

    private final Path dir;
    private volatile Map<String, SupplierSpec> suppliers = Map.of();
    private volatile long lastLoaded = 0;

    public SupplierRegistry(@Value("${notify.suppliers-dir:./suppliers}") String suppliersDir) {
        this.dir = Path.of(suppliersDir);
    }

    @PostConstruct
    public void init() {
        reload();
        if (suppliers.isEmpty()) {
            log.warn("No supplier definitions loaded from {}", dir.toAbsolutePath());
        }
    }

    /** 定期热加载：文件增删改自动生效（对齐 Apollo 动态配置思想） */
    public synchronized void reloadIfStale() {
        try {
            long newest = latestMtime();
            if (newest != lastLoaded) {
                reload();
            }
        } catch (Exception e) {
            log.error("Supplier hot-reload failed", e);
        }
    }

    private long latestMtime() throws Exception {
        if (!Files.isDirectory(dir)) return 0;
        try (Stream<Path> s = Files.list(dir)) {
            return s.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .mapToLong(p -> {
                        try { return Files.getLastModifiedTime(p).toMillis(); }
                        catch (Exception e) { return 0; }
                    })
                    .max().orElse(0);
        }
    }

    private synchronized void reload() {
        if (!Files.isDirectory(dir)) {
            log.warn("suppliers dir not found: {}", dir.toAbsolutePath());
            return;
        }
        try (Stream<Path> s = Files.list(dir)) {
            List<Path> files = s.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .toList();
            Map<String, SupplierSpec> loaded = files.stream()
                    .map(p -> {
                        try {
                            return SupplierSpec.load(p);
                        } catch (Exception e) {
                            log.error("Failed to load supplier spec {}", p, e);
                            return null;
                        }
                    })
                    .filter(spec -> spec != null)
                    .collect(Collectors.toMap(SupplierSpec::name, x -> x));
            this.suppliers = Map.copyOf(loaded);
            this.lastLoaded = latestMtime();
            log.info("Loaded {} suppliers: {}", suppliers.size(), suppliers.keySet());
        } catch (Exception e) {
            log.error("Reload suppliers failed", e);
        }
    }

    public SupplierSpec get(String name) {
        return suppliers.get(name);
    }

    public List<String> names() {
        return List.copyOf(suppliers.keySet());
    }
}
