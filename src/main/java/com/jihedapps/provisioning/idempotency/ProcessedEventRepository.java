package com.jihedapps.provisioning.idempotency;

import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;

@Repository
public class ProcessedEventRepository {

    private final JdbcTemplate jdbc;
    private final String insertQuery;

    public ProcessedEventRepository(JdbcTemplate jdbc,
                                    @org.springframework.beans.factory.annotation.Value("${provisioning.queries.insert-processed-event:INSERT INTO processed_events (event_id, topic, aggregate_id, processed_at) VALUES (?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING}") String insertQuery) {
        this.jdbc = jdbc;
        this.insertQuery = insertQuery;
    }

    /**
     * Tries to insert the event into the processed_events table.
     * Returns true if successful, false if the eventId already exists (DuplicateKeyException).
     * This relies entirely on the database UNIQUE constraint for idempotency, avoiding race conditions.
     */
    public boolean markProcessed(String eventId, String topic, String aggregateId) {
        // executeUpdate returns 1 if inserted, 0 if it already existed (ON CONFLICT DO NOTHING)
        int rows = jdbc.update(insertQuery, eventId, topic, aggregateId, Timestamp.from(Instant.now()));
        return rows > 0;
    }
}
