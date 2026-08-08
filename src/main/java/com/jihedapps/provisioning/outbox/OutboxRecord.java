package com.jihedapps.provisioning.outbox;

import java.time.Instant;

public record OutboxRecord(
        String id,
        String aggregateId,
        String eventType,
        String payload,
        Instant createdAt,
        Instant publishedAt,
        int attempts,
        String lastError,
        String traceContext
) {
}
