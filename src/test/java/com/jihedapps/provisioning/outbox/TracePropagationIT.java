package com.jihedapps.provisioning.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.ProvisioningApplication;
import com.jihedapps.provisioning.kafka.OutboxPortabilityEventPublisher;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.actuate.autoconfigure.tracing.otlp.OtlpAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
        classes = {ProvisioningApplication.class, TracePropagationIT.TestTracingConfig.class},
        properties = {
                "management.tracing.enabled=true",
                "provisioning.outbox.relay.enabled=false",
                "spring.autoconfigure.exclude=org.springframework.boot.actuate.autoconfigure.tracing.otlp.OtlpAutoConfiguration"
        }
)
@ActiveProfiles("postgres")
class TracePropagationIT {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    static void startContainers() {
        postgres.start();
        kafka.start();
    }

    @AfterAll
    static void stopContainers() {
        postgres.stop();
        kafka.stop();
    }

    @TestConfiguration
    static class TestTracingConfig {
        @Bean
        InMemorySpanExporter inMemorySpanExporter() {
            return InMemorySpanExporter.create();
        }

        @Bean
        SpanProcessor simpleSpanProcessor(InMemorySpanExporter exporter) {
            return SimpleSpanProcessor.create(exporter);
        }
    }

    @Autowired
    OutboxPortabilityEventPublisher publisher;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    OutboxRepository repository;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    TransactionTemplate txTemplate;

    @Autowired
    Tracer tracer;

    @Autowired
    InMemorySpanExporter inMemorySpanExporter;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        inMemorySpanExporter.reset();
    }

    @Test
    void shouldPersistTraceContextAndPropagateHeaderToKafkaConsumer() {
        String requestId = UUID.randomUUID().toString();
        Span parentSpan = tracer.nextSpan().name("test-saga-start").start();

        String traceId;
        try (Tracer.SpanInScope scope = tracer.withSpan(parentSpan)) {
            traceId = parentSpan.context().traceId();
            txTemplate.executeWithoutResult(status ->
                    publisher.publish("PORTABILITY_REQUESTED", requestId, Map.of("msisdn", "+21620000000"))
            );
        } finally {
            parentSpan.end();
        }

        // 1. Verify trace_context column in DB
        String traceContextJson = jdbc.queryForObject(
                "SELECT trace_context FROM portability_outbox WHERE aggregate_id = ?",
                String.class, requestId);
        assertThat(traceContextJson).isNotNull().contains(traceId);

        // 2. Relay publish
        outboxRelay.publishBatch();

        // 3. Kafka consumer assertion
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList("number-portability-events"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertThat(records).isNotEmpty();

            boolean traceHeaderFound = false;
            for (ConsumerRecord<String, String> record : records) {
                var traceparentHeader = record.headers().lastHeader("traceparent");
                if (traceparentHeader != null) {
                    String headerVal = new String(traceparentHeader.value());
                    if (headerVal.contains(traceId)) {
                        traceHeaderFound = true;
                        break;
                    }
                }
            }
            assertThat(traceHeaderFound).isTrue();
        });
        consumer.close();
    }

    @Test
    void shouldCreateChildSpanWithParentInTraceHistory() {
        String requestId = UUID.randomUUID().toString();
        Span parentSpan = tracer.nextSpan().name("originating-saga-span").start();

        String traceId = parentSpan.context().traceId();
        String parentSpanId = parentSpan.context().spanId();

        try (Tracer.SpanInScope scope = tracer.withSpan(parentSpan)) {
            txTemplate.executeWithoutResult(status ->
                    publisher.publish("PORTABILITY_REQUESTED", requestId, Map.of("msisdn", "+21620000001"))
            );
        } finally {
            parentSpan.end();
        }

        outboxRelay.publishBatch();

        List<SpanData> finishedSpans = inMemorySpanExporter.getFinishedSpanItems();
        assertThat(finishedSpans).isNotEmpty();

        // Verify there is a span belonging to the same traceId that has parentSpanId as parent
        SpanData childSpan = finishedSpans.stream()
                .filter(s -> s.getTraceId().equals(traceId) && s.getParentSpanId().equals(parentSpanId))
                .findFirst()
                .orElse(null);

        assertThat(childSpan).isNotNull();
    }

    @Test
    void shouldPreserveTraceIdAcrossBrokerOutageAndRetry() {
        String requestId = UUID.randomUUID().toString();
        Span parentSpan = tracer.nextSpan().name("resilient-saga-span").start();
        String traceId = parentSpan.context().traceId();

        try (Tracer.SpanInScope scope = tracer.withSpan(parentSpan)) {
            txTemplate.executeWithoutResult(status ->
                    publisher.publish("PORTABILITY_REQUESTED", requestId, Map.of("msisdn", "+21620000002"))
            );
        } finally {
            parentSpan.end();
        }

        // Verify record is created with attempts=0
        Integer attemptsBefore = jdbc.queryForObject(
                "SELECT attempts FROM portability_outbox WHERE aggregate_id = ?", Integer.class, requestId);
        assertThat(attemptsBefore).isEqualTo(0);

        // Pause Kafka to simulate outage without changing container port
        try {
            DockerClientFactory.lazyClient().pauseContainerCmd(kafka.getContainerId()).exec();
            
            // Attempt publish -> should fail and increment attempts
            outboxRelay.publishBatch();
        } finally {
            DockerClientFactory.lazyClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        Integer attemptsAfterFail = jdbc.queryForObject(
                "SELECT attempts FROM portability_outbox WHERE aggregate_id = ?", Integer.class, requestId);
        assertThat(attemptsAfterFail).isEqualTo(1);

        // Attempt publish -> should succeed now
        outboxRelay.publishBatch();

        String publishedAt = jdbc.queryForObject(
                "SELECT published_at FROM portability_outbox WHERE aggregate_id = ?", String.class, requestId);
        assertThat(publishedAt).isNotNull();

        // Verify header in Kafka still contains original traceId
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Collections.singletonList("number-portability-events"));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            boolean traceHeaderFound = false;
            for (ConsumerRecord<String, String> record : records) {
                var traceparentHeader = record.headers().lastHeader("traceparent");
                if (traceparentHeader != null && new String(traceparentHeader.value()).contains(traceId)) {
                    traceHeaderFound = true;
                    break;
                }
            }
            assertThat(traceHeaderFound).isTrue();
        });
        consumer.close();
    }

    private KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(
                Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
                )
        );
    }
}
