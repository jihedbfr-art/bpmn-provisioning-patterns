package com.jihedapps.provisioning.outbox;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
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
public class OutboxRelay {

    private static final Logger LOG = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;
    private final ObjectMapper mapper;
    private final String topic;
    private final int batchSize;
    private final int metricsInterval;
    private final boolean enabled;
    
    private int cycleCount = 0;
    private final Duration sendTimeout;
    private final java.util.concurrent.atomic.AtomicInteger pendingGauge;
    private final java.util.concurrent.atomic.AtomicInteger deadGauge;

    public OutboxRelay(OutboxRepository repository,
                       KafkaTemplate<String, String> kafkaTemplate,
                       JdbcTemplate jdbcTemplate,
                       MeterRegistry meterRegistry,
                       ObjectProvider<Tracer> tracerProvider,
                       ObjectProvider<Propagator> propagatorProvider,
                       ObjectMapper mapper,
                       @Value("${provisioning.kafka.topic:number-portability-events}") String topic,
                       @Value("${provisioning.outbox.relay.send-timeout:PT6S}") Duration sendTimeout,
                       @Value("${provisioning.outbox.relay.batch-size:10}") int batchSize,
                       @Value("${provisioning.outbox.relay.metrics-interval:30}") int metricsInterval,
                       @Value("${provisioning.outbox.relay.enabled:true}") boolean enabled) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
        this.mapper = mapper;
        this.topic = topic;
        this.sendTimeout = sendTimeout;
        this.batchSize = batchSize;
        this.metricsInterval = metricsInterval;
        this.enabled = enabled;

        this.pendingGauge = meterRegistry.gauge("provisioning.outbox.pending", new java.util.concurrent.atomic.AtomicInteger(0));
        this.deadGauge = meterRegistry.gauge("provisioning.outbox.dead", new java.util.concurrent.atomic.AtomicInteger(0));
    }

    /**
     * Polling publisher reading the outbox table.
     * Note: SKIP LOCKED skips locked rows to allow parallel processing across multiple relay instances,
     * which does not guarantee strict global event ordering across partitions, only at-least-once delivery.
     */
    @Scheduled(fixedDelayString = "${provisioning.outbox.relay.interval:PT1S}")
    public void scheduledPublishBatch() {
        if (!enabled) {
            return;
        }
        publishBatch();
    }

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

        Tracer tracer = tracerProvider.getIfAvailable(() -> Tracer.NOOP);
        Propagator propagator = propagatorProvider.getIfAvailable(() -> Propagator.NOOP);

        for (OutboxRecord record : batch) {
            Span span = null;
            Tracer.SpanInScope scope = null;
            try {
                if (record.traceContext() != null && !record.traceContext().isBlank()) {
                    try {
                        java.util.Map<String, String> carrier = mapper.readValue(record.traceContext(), new TypeReference<>() {});
                        Span.Builder builder = propagator.extract(carrier, java.util.Map::get);
                        span = builder.name("outbox-publish").start();
                        span.tag("messaging.destination", topic);
                        span.tag("outbox.attempts", String.valueOf(record.attempts() + 1));
                        scope = tracer.withSpan(span);
                    } catch (Exception e) {
                        LOG.warn("Failed to extract trace context for record {}", record.id(), e);
                    }
                }

                // Synchronous send to ensure we don't mark as published if the broker is down
                kafkaTemplate.send(topic, record.aggregateId(), record.payload())
                             .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                repository.markPublished(record.id());
            } catch (Exception e) {
                LOG.error("Failed to publish outbox record {}", record.id(), e);
                repository.markFailed(record.id(), e.getMessage());
                // A deterministically failing record (e.g., payload too large) blocks the head of the queue for max-attempts cycles.
                // This is an acceptable tradeoff: if the broker is unreachable, we exit early to limit transaction time.
                // Replacing this with 'continue' would prevent global transaction timeouts, but it would ruin attempt counting
                // if the broker is actually down for the entire batch.
                break; // Stop processing batch if broker is down to prevent transaction timeout
            } finally {
                if (scope != null) {
                    scope.close();
                }
                if (span != null) {
                    span.end();
                }
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
