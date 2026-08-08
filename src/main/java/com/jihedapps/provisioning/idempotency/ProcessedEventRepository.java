package com.jihedapps.provisioning.idempotency;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;

    public ProcessedEventRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Tries to insert the event into the processed_events table.
     * Returns true if successful, false if the eventId already exists (DuplicateKeyException).
     * This relies entirely on the database UNIQUE constraint for idempotency, avoiding race conditions.
     */
    public boolean markProcessed(String eventId, String topic, String aggregateId) {
        try {
            jdbc.update("INSERT INTO processed_events (event_id, topic, aggregate_id, processed_at) VALUES (?, ?, ?, ?)",
                    eventId, topic, aggregateId, Timestamp.from(Instant.now()));
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }
}
