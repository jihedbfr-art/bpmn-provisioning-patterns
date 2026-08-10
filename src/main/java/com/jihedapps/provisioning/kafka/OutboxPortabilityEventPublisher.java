package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.outbox.OutboxRecord;
import com.jihedapps.provisioning.outbox.OutboxRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;

@Component
public class OutboxPortabilityEventPublisher implements PortabilityEventPublisher {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper mapper;
    private final Tracer tracer;
    private final Propagator propagator;

    public OutboxPortabilityEventPublisher(OutboxRepository outboxRepository,
                                         ObjectMapper mapper,
                                         @org.springframework.context.annotation.Lazy Tracer tracer,
                                         @org.springframework.context.annotation.Lazy Propagator propagator) {
        this.outboxRepository = outboxRepository;
        this.mapper = mapper;
        this.tracer = tracer;
        this.propagator = propagator;
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

        Map<String, String> carrier = new HashMap<>();
        if (tracer.currentTraceContext().context() != null) {
            propagator.inject(tracer.currentTraceContext().context(), carrier, Map::put);
        }
        
        String traceContextJson = null;
        if (!carrier.isEmpty()) {
            try {
                traceContextJson = mapper.writeValueAsString(carrier);
            } catch (Exception e) {
                // Ignore serialization error, trace context is optional
            }
        }

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
                    null,
                    traceContextJson
            );
            outboxRepository.insert(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize or insert outbox event", e);
        }
    }
}
