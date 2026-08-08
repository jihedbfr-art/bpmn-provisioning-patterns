package com.jihedapps.provisioning.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "provisioning.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final String topic;
    private final int batchSize;
    private final int metricsInterval;
    
    private int cycleCount = 0;
    private final Duration sendTimeout;
    private final java.util.concurrent.atomic.AtomicInteger pendingGauge;
    private final java.util.concurrent.atomic.AtomicInteger deadGauge;

    public OutboxRelay(OutboxRepository repository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       JdbcTemplate jdbcTemplate,
                       MeterRegistry meterRegistry,
                       @Value("${provisioning.kafka.topic:number-portability-events}") String topic,
                       @Value("${provisioning.outbox.relay.send-timeout:PT6S}") Duration sendTimeout,
                       @Value("${provisioning.outbox.relay.batch-size:10}") int batchSize,
                       @Value("${provisioning.outbox.relay.metrics-interval:30}") int metricsInterval) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.topic = topic;
        this.sendTimeout = sendTimeout;
        this.batchSize = batchSize;
        this.metricsInterval = metricsInterval;

        this.pendingGauge = meterRegistry.gauge("provisioning.outbox.pending", new java.util.concurrent.atomic.AtomicInteger(0));
        this.deadGauge = meterRegistry.gauge("provisioning.outbox.dead", new java.util.concurrent.atomic.AtomicInteger(0));
    }

    /**
     * Polling publisher reading the outbox table.
     * Note: Ordering by aggregate_id is roughly preserved by SKIP LOCKED, but multi-instance relays
     * could interleave messages. The broker partition key will maintain order for what it receives,
     * but we do not guarantee strict global event ordering here, only at-least-once delivery.
     */
    @Scheduled(fixedDelayString = "${provisioning.outbox.relay.interval:PT1S}")
    @Transactional(timeout = 30)
    public void publishBatch() {
        if (cycleCount++ % metricsInterval == 0) {
            updateMetrics();
        }

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
                // Un enregistrement en échec déterministe (payload trop gros, etc.) bloque la tête de file pendant max-attempts cycles.
                // C'est le bon compromis : si le broker est injoignable, on sort pour limiter le temps de transaction.
                // Le remplacer par 'continue' empêcherait le timeout de transaction global, mais ruinerait l'incrémentation des tentatives
                // si le broker est réellement indisponible pour tout le batch.
                break; // Stop processing batch if broker is down to prevent transaction timeout
            }
        }
    }

    private void updateMetrics() {
        Integer pending = jdbcTemplate.queryForObject("SELECT count(*) FROM portability_outbox WHERE published_at IS NULL AND failed_at IS NULL", Integer.class);
        if (pending != null) {
            pendingGauge.set(pending);
        }
        Integer dead = jdbcTemplate.queryForObject("SELECT count(*) FROM portability_outbox WHERE failed_at IS NOT NULL", Integer.class);
        if (dead != null) {
            deadGauge.set(dead);
        }
    }
}
