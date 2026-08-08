package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @deprecated Replaced by {@link OutboxPortabilityEventPublisher}.
 * This direct Kafka publisher caused dual-write issues because it published outside the Camunda transaction.
 * The new outbox publisher writes to the database within the same transaction to guarantee atomicity.
 */
@Deprecated
public class KafkaPortabilityEventPublisher implements PortabilityEventPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaPortabilityEventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;
    private final String topic;

    public KafkaPortabilityEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                           ObjectMapper mapper,
                                           @Value("${provisioning.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.topic = topic;
    }

    @Override
    public void publish(String eventType, String requestId, Map<String, Object> payload) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventType", eventType);
        envelope.put("requestId", requestId);
        envelope.put("occurredAt", Instant.now().toString());
        envelope.put("payload", payload);

        try {
            String json = mapper.writeValueAsString(envelope);
            kafkaTemplate.send(topic, requestId, json);
        } catch (Exception e) {
            // a saga step already committed process state before this publish runs — losing the
            // downstream notification is bad, but throwing here would leave the process instance
            // stuck retrying a step that already succeeded. log loud, don't fail the saga.
            LOG.error("failed to publish {} event for request {}", eventType, requestId, e);
        }
    }
}
