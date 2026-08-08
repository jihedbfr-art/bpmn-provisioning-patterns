package com.jihedapps.provisioning.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@EnableScheduling
@ConditionalOnProperty(name = "provisioning.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final int batchSize;
    private final Duration sendTimeout;

    public OutboxRelay(OutboxRepository repository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       JdbcTemplate jdbcTemplate,
                       MeterRegistry meterRegistry,
                       @Value("${provisioning.kafka.topic}") String topic,
                       @Value("${provisioning.outbox.relay.batch-size:50}") int batchSize,
                       @Value("${provisioning.outbox.relay.send-timeout:PT5S}") Duration sendTimeout) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.batchSize = batchSize;
        this.sendTimeout = sendTimeout;

        Gauge.builder("provisioning.outbox.pending", () -> 
                jdbcTemplate.queryForObject("SELECT count(*) FROM portability_outbox WHERE published_at IS NULL", Integer.class))
             .description("Number of pending outbox events")
             .register(meterRegistry);
    }

    /**
     * Polling publisher reading the outbox table.
     * Note: Ordering by aggregate_id is roughly preserved by SKIP LOCKED, but multi-instance relays
     * could interleave messages. The broker partition key will maintain order for what it receives,
     * but we do not guarantee strict global event ordering here, only at-least-once delivery.
     */
    @Scheduled(fixedDelayString = "${provisioning.outbox.relay.interval:PT1S}")
    @Transactional
    public void publishBatch() {
        List<OutboxRecord> batch = repository.lockUnpublishedBatch(batchSize);
        if (batch.isEmpty()) {
            return;
        }

        LOG.debug("Publishing {} outbox records", batch.size());

        for (OutboxRecord record : batch) {
            try {
                // Synchronous send to ensure we don't mark as published if the broker is down
                kafkaTemplate.send(topic, record.aggregateId(), record.payload())
                             .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                repository.markPublished(record.id());
            } catch (Exception e) {
                LOG.error("Failed to publish outbox record {}", record.id(), e);
                repository.markFailed(record.id(), e.getMessage());
            }
        }
    }
}
