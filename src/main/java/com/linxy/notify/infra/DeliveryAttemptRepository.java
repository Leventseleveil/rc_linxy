package com.linxy.notify.infra;

import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * 投递审计：每次出站 HTTP 尝试一条记录，无论成败。
 * 对齐 xhs push mockingbird 的 msgTrace 思想 —— 可回答
 * "这条通知到底发没发、发了几次、对方回了什么"。
 */
@Repository
public class DeliveryAttemptRepository {

    public record Attempt(String id, String notificationId, int attemptNo, String endpoint,
                          String requestDigest, Integer httpStatus, String outcome,
                          Long durationMs, String errorNote, Instant createdAt) {}

    private final JdbcTemplate jdbc;

    public DeliveryAttemptRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(Attempt a) {
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                "INSERT INTO delivery_attempt (id, notification_id, attempt_no, endpoint, request_digest, http_status, outcome, duration_ms, error_note, created_at) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?)");
            ps.setString(1, a.id());
            ps.setString(2, a.notificationId());
            ps.setInt(3, a.attemptNo());
            ps.setString(4, a.endpoint());
            ps.setString(5, a.requestDigest());
            if (a.httpStatus() == null) ps.setNull(6, java.sql.Types.INTEGER); else ps.setInt(6, a.httpStatus());
            ps.setString(7, a.outcome());
            if (a.durationMs() == null) ps.setNull(8, java.sql.Types.BIGINT); else ps.setLong(8, a.durationMs());
            ps.setString(9, a.errorNote());
            ps.setString(10, a.createdAt().toString());
            return ps;
        });
    }

    public List<Attempt> findByNotification(String notificationId) {
        return jdbc.query(
            "SELECT * FROM delivery_attempt WHERE notification_id = ? ORDER BY attempt_no",
            (rs, i) -> new Attempt(
                rs.getString("id"), rs.getString("notification_id"), rs.getInt("attempt_no"),
                rs.getString("endpoint"), rs.getString("request_digest"),
                rs.getObject("http_status") == null ? null : rs.getInt("http_status"),
                rs.getString("outcome"),
                rs.getObject("duration_ms") == null ? null : rs.getLong("duration_ms"),
                rs.getString("error_note"), Instant.parse(rs.getString("created_at"))),
            notificationId);
    }
}
