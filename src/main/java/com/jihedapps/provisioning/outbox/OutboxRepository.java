package com.jihedapps.provisioning.outbox;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbc;
    private final String selectQuery;

    private final int maxAttempts;

    public OutboxRepository(JdbcTemplate jdbc,
                            @Value("${provisioning.outbox.select-query:SELECT * FROM portability_outbox WHERE published_at IS NULL AND attempts < ? ORDER BY created_at LIMIT ? FOR UPDATE SKIP LOCKED}") String selectQuery,
                            @Value("${provisioning.outbox.relay.max-attempts:10}") int maxAttempts) {
        this.jdbc = jdbc;
        this.selectQuery = selectQuery;
        this.maxAttempts = maxAttempts;
    }

    public void insert(OutboxRecord record) {
        jdbc.update("INSERT INTO portability_outbox (id, aggregate_id, event_type, payload, created_at, published_at, attempts, last_error) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                record.id(),
                record.aggregateId(),
                record.eventType(),
                record.payload(),
                record.createdAt() != null ? Timestamp.from(record.createdAt()) : null,
                record.publishedAt() != null ? Timestamp.from(record.publishedAt()) : null,
                record.attempts(),
                record.lastError());
    }

    public List<OutboxRecord> lockUnpublishedBatch(int limit) {
        return jdbc.query(selectQuery, outboxRowMapper, maxAttempts, limit);
    }

    public void markPublished(String id) {
        jdbc.update("UPDATE portability_outbox SET published_at = ? WHERE id = ?", Timestamp.from(Instant.now()), id);
    }

    public void markFailed(String id, String error) {
        jdbc.update("UPDATE portability_outbox SET attempts = attempts + 1, last_error = ?, failed_at = CASE WHEN attempts + 1 >= ? THEN ? ELSE NULL END WHERE id = ?", 
                error, maxAttempts, Timestamp.from(Instant.now()), id);
    }

    private final RowMapper<OutboxRecord> outboxRowMapper = (rs, rowNum) -> new OutboxRecord(
            rs.getString("id"),
            rs.getString("aggregate_id"),
            rs.getString("event_type"),
            rs.getString("payload"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("published_at") != null ? rs.getTimestamp("published_at").toInstant() : null,
            rs.getInt("attempts"),
            rs.getString("last_error")
    );
}
