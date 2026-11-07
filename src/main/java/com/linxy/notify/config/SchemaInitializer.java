package com.linxy.notify.config;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import jakarta.annotation.PostConstruct;

/**
 * 启动时执行 schema（幂等 DDL）。
 * SQLite 场景下还负责建数据目录；生产换 Flyway/Liquibase 做版本化迁移。
 */
@Configuration
public class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);
    private final DataSource dataSource;

    public SchemaInitializer(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void init() throws Exception {
        String url = dataSource.getConnection().getMetaData().getURL();
        if (url.startsWith("jdbc:sqlite:")) {
            String path = url.substring("jdbc:sqlite:".length());
            Path dbPath = Path.of(path);
            if (dbPath.getParent() != null) {
                Files.createDirectories(dbPath.getParent());
            }
        }
        String sql = new String(new ClassPathResource("db/schema.sql").getInputStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8);
        try (Connection con = dataSource.getConnection(); var stmts = con.createStatement()) {
            for (String one : sql.split(";")) {
                String s = one.trim();
                if (s.isEmpty()) continue;
                stmts.execute(s);
            }
        }
        // SQLite WAL：读写并发更平滑
        try (Connection con = dataSource.getConnection(); var st = con.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA busy_timeout=3000");
            st.execute("PRAGMA foreign_keys=ON");
        }
        log.info("schema initialized");
    }
}
