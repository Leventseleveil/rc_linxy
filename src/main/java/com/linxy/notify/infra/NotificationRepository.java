package com.linxy.notify.infra;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

import com.linxy.notify.domain.Notification;

/**
 * 通知存储。SQLite 单文件实现，SQL 保守兼容（无 UPSERT 方言依赖），
 * 生产换 MySQL/PG 只需替换 INSERT 冲突处理为 ON DUPLICATE KEY / ON CONFLICT。
 */
@Repository
public class NotificationRepository {

    private final JdbcTemplate jdbc;

    public NotificationRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final RowMapper<Notification> MAPPER = (rs, i) -> {
        Notification n = new Notification();
        n.setId(rs.getString("id"));
        n.setSource(rs.getString("source"));
        n.setSupplier(rs.getString("supplier"));
        n.setEventType(rs.getString("event_type"));
        n.setPayload(rs.getString("payload"));
        String dk = rs.getString("dedup_key");
        n.setDedupKey(rs.wasNull() ? null : dk);
        n.setStatus(Notification.Status.valueOf(rs.getString("status")));
        n.setAttempt(rs.getInt("attempt"));
        n.setMaxAttempts(rs.getInt("max_attempts"));
        String na = rs.getString("next_attempt_at");
        n.setNextAttemptAt(na == null ? null : Instant.parse(na));
        n.setCreatedAt(Instant.parse(rs.getString("created_at")));
        n.setUpdatedAt(Instant.parse(rs.getString("updated_at")));
        String dv = rs.getString("delivered_at");
        n.setDeliveredAt(dv == null ? null : Instant.parse(dv));
        n.setLastError(rs.getString("last_error"));
        return n;
    };

    /**
     * 插入；唯一索引冲突（同 source+dedup_key 重复提交）返回 false 而不是抛异常，
     * 让上层走幂等分支。SQLite 下部分索引保证 dedup_key 为 NULL 的行不参与约束。
     */
    public boolean insert(Notification n) {
        try {
            int rows = jdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO notification (id, source, supplier, event_type, payload, dedup_key, status, attempt, max_attempts, next_attempt_at, created_at, updated_at) "
                    + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
                ps.setString(1, n.getId());
                ps.setString(2, n.getSource());
                ps.setString(3, n.getSupplier());
                ps.setString(4, n.getEventType());
                ps.setString(5, n.getPayload());
                ps.setString(6, n.getDedupKey());
                ps.setString(7, n.getStatus().name());
                ps.setInt(8, n.getAttempt());
                ps.setInt(9, n.getMaxAttempts());
                ps.setString(10, n.getNextAttemptAt() == null ? null : n.getNextAttemptAt().toString());
                ps.setString(11, n.getCreatedAt().toString());
                ps.setString(12, n.getUpdatedAt().toString());
                return ps;
            });
            return rows == 1;
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            return false;
        } catch (org.springframework.jdbc.UncategorizedSQLException e) {
            // SQLite JDBC 的 UNIQUE 冲突未被 Spring 完整翻译，按错误码兜底识别
            Throwable root = e.getCause();
            if (root instanceof java.sql.SQLException se
                    && (se.getMessage() != null && se.getMessage().contains("UNIQUE"))) {
                return false;
            }
            throw e;
        }
    }

    public Optional<Notification> findById(String id) {
        List<Notification> l = jdbc.query("SELECT * FROM notification WHERE id = ?", MAPPER, id);
        return l.isEmpty() ? Optional.empty() : Optional.of(l.get(0));
    }

    public Optional<Notification> findByDedup(String source, String dedupKey) {
        List<Notification> l = jdbc.query(
            "SELECT * FROM notification WHERE source = ? AND dedup_key = ?", MAPPER, source, dedupKey);
        return l.isEmpty() ? Optional.empty() : Optional.of(l.get(0));
    }

    /**
     * 捞取到期待投递的通知（扫描式调度核心查询）。
     * FOR UPDATE SKIP LOCKED 语义在 SQLite 下由写锁天然覆盖（单写者），
     * 多实例部署换 MySQL/PG 时应显式加行锁避免重复投递。
     */
    public List<Notification> findDue(int limit, Instant now) {
        return jdbc.query(
            "SELECT * FROM notification WHERE status IN ('PENDING','RETRYING') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) "
            + "ORDER BY next_attempt_at LIMIT ?",
            MAPPER, now.toString(), limit);
    }

    public int updateStatus(String id, Notification.Status status, Instant nextAttemptAt, String lastError) {
        return jdbc.update(
            "UPDATE notification SET status = ?, next_attempt_at = ?, last_error = ?, updated_at = ?, attempt = attempt WHERE id = ?",
            status.name(),
            nextAttemptAt == null ? null : nextAttemptAt.toString(),
            lastError,
            Instant.now().toString(),
            id);
    }

    /** 投递成功：终态落库（成功这次也算一次 attempt） */
    public int markDelivered(String id, Instant deliveredAt) {
        return jdbc.update(
            "UPDATE notification SET status = 'DELIVERED', delivered_at = ?, updated_at = ?, attempt = attempt + 1, next_attempt_at = NULL WHERE id = ?",
            deliveredAt.toString(), Instant.now().toString(), id);
    }

    /** 一次尝试后：attempt+1，按结果推进状态机 */
    public int recordAttempt(String id, Notification.Status next, Instant nextAttemptAt, String err) {
        return jdbc.update(
            "UPDATE notification SET attempt = attempt + 1, status = ?, next_attempt_at = ?, last_error = ?, updated_at = ? WHERE id = ?",
            next.name(),
            nextAttemptAt == null ? null : nextAttemptAt.toString(),
            err,
            Instant.now().toString(),
            id);
    }

    /** 死信重发：回到 PENDING，attempt 清零，重新走完整退避 */
    public int redeliver(String id) {
        return jdbc.update(
            "UPDATE notification SET status = 'PENDING', attempt = 0, next_attempt_at = ?, updated_at = ?, last_error = 'redelivered' WHERE id = ? AND status = 'DEAD'",
            Instant.now().toString(), Instant.now().toString(), id);
    }

    public List<Notification> findDead(int limit) {
        return jdbc.query(
            "SELECT * FROM notification WHERE status = 'DEAD' ORDER BY updated_at DESC LIMIT ?",
            MAPPER, limit);
    }
}
