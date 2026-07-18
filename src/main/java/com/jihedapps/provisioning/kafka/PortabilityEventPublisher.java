package com.jihedapps.provisioning.kafka;

/**
 * Kept as an interface so the Camunda process unit tests (fast, no Docker) can run against a
 * stub while {@link KafkaPortabilityEventPublisher} — exercised for real in
 * {@code PortabilityEventPublisherIT} — proves the actual Kafka wiring works.
 */
public interface PortabilityEventPublisher {

    void publish(String eventType, String requestId, java.util.Map<String, Object> payload);
}
