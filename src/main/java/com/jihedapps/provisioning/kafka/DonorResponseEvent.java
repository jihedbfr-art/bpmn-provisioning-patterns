package com.jihedapps.provisioning.kafka;

import java.time.Instant;

public record DonorResponseEvent(
        String eventId,
        String requestId,
        String decision,
        Instant occurredAt
) {
}
