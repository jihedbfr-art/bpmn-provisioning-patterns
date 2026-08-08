package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.outbox.OutboxRecord;
import com.jihedapps.provisioning.outbox.OutboxRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Component
public class OutboxPortabilityEventPublisher implements PortabilityEventPublisher {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper mapper;

    public OutboxPortabilityEventPublisher(OutboxRepository outboxRepository, ObjectMapper mapper) {
        this.outboxRepository = outboxRepository;
        this.mapper = mapper;
    }

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void publish(String eventType, String requestId, Map<String, Object> payload) {
        String eventId = UUID.randomUUID().toString();
        
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", eventId);
        envelope.put("eventType", eventType);
        envelope.put("requestId", requestId);
        envelope.put("occurredAt", Instant.now().toString());
        envelope.put("payload", payload);

        try {
            String json = mapper.writeValueAsString(envelope);
            OutboxRecord record = new OutboxRecord(
                    eventId,
                    requestId,
                    eventType,
                    json,
                    Instant.now(),
                    null,
                    0,
                    null
            );
            outboxRepository.insert(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize or insert outbox event", e);
        }
    }
}
