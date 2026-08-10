package com.jihedapps.provisioning.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.ProvisioningApplication;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.camunda.bpm.engine.RuntimeService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ProvisioningApplication.class, properties = {
        "provisioning.outbox.relay.enabled=false",
        "camunda.bpm.job-execution.enabled=false",
        "provisioning.outbox.relay.max-attempts=3",
        "spring.kafka.producer.properties.delivery.timeout.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=500",
        "spring.kafka.producer.properties.max.block.ms=500"
})
@ActiveProfiles("postgres")
class OutboxRelayIT {

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

    @Autowired
    RuntimeService runtimeService;

    @Autowired
    OutboxRelay outboxRelay;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ObjectMapper mapper;

    @BeforeEach
    void clearOutbox() {
        jdbc.execute("TRUNCATE TABLE portability_outbox");
        await().atMost(Duration.ofSeconds(15)).ignoreExceptions().untilAsserted(() -> {
            try (KafkaConsumer<String, String> c = createConsumer()) {
                assertThat(c.partitionsFor("number-portability-events")).isNotEmpty();
            }
        });
    }

    @Test
    void shouldPublishSuccessfullyWhenBrokerIsUp() throws Exception {
        KafkaConsumer<String, String> consumer = createConsumer();
        String req1 = UUID.randomUUID().toString();
        
        runtimeService.startProcessInstanceByKey("number-portability-saga", req1, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT30M"
        ));

        outboxRelay.publishBatch();

        Integer afterPublish = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", Integer.class, req1);
        assertThat(afterPublish).isEqualTo(1);

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertThat(records.isEmpty()).isFalse();
            ConsumerRecord<String, String> rec = records.iterator().next();
            assertThat(rec.key()).isEqualTo(req1);
            JsonNode node = mapper.readTree(rec.value());
            assertThat(node.get("eventType").asText()).isEqualTo("PortabilityRequestedEvent");
        });
        
        consumer.close();
    }

    @Test
    void shouldRetryAndPublishWhenBrokerRecovers() throws Exception {
        KafkaConsumer<String, String> consumer = createConsumer();
        
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();

        String req2 = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey("number-portability-saga", req2, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT30M"
        ));

        try {
            System.out.println("DEBUG: records before publishBatch: " + jdbc.queryForList("SELECT * FROM portability_outbox"));
            // Attempt 1 fails
            outboxRelay.publishBatch();
            System.out.println("DEBUG: records after publishBatch: " + jdbc.queryForList("SELECT * FROM portability_outbox"));

            Map<String, Object> record2 = jdbc.queryForMap("SELECT attempts, last_error FROM portability_outbox WHERE aggregate_id = ?", req2);
            assertThat((Integer) record2.get("attempts")).isEqualTo(1);
            assertThat(record2.get("last_error")).isNotNull();
        } finally {
            // Restore broker
            kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }

        // Keep trying to publish until it succeeds (Kafka might take a moment to be fully ready)
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            outboxRelay.publishBatch();
            Integer count = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", Integer.class, req2);
            assertThat(count).isEqualTo(1);
        });

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            boolean found = false;
            for (ConsumerRecord<String, String> r : records) {
                if (req2.equals(r.key())) found = true;
            }
            assertThat(found).isTrue();
        });
        
        consumer.close();
    }
    
    @Test
    void shouldStopBatchAndMarkFailedWhenBrokerIsDown() throws Exception {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();

        try {
            // Create 15 events in outbox
            for (int i = 0; i < 15; i++) {
                String req = UUID.randomUUID().toString();
                runtimeService.startProcessInstanceByKey("number-portability-saga", req, Map.of(
                        "msisdn", "+21620000000",
                        "donorOperator", "Ooredoo",
                        "recipientOperator", "Orange",
                        "donorResponseTimeout", "PT30M"
                ));
            }

            Integer pendingBefore = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE published_at IS NULL", Integer.class);
            assertThat(pendingBefore).isEqualTo(15);

            // maxAttempts = 3, we have 15 events.
            // When broker is down, each publishBatch() fails on the FIRST event of the batch and breaks.
            // So if we call it 3 times, the oldest event reaches attempts=3 and gets failed_at set.
            // If we call it 11 times, at least 3 events should reach max attempts and be dead,
            // while the remaining events are still pending and attempts < 3.
            
            for (int i = 0; i < 11; i++) {
                outboxRelay.publishBatch();
            }

            Integer deadCount = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE failed_at IS NOT NULL", Integer.class);
            assertThat(deadCount).isGreaterThan(0);
            
            // Specifically, with 11 calls and max_attempts=3, the oldest events should have failed_at set
            // The exact number depends on how batches are read, but at least one must be dead.
            // 11 calls / 3 attempts = 3 dead events, and the 4th has 2 attempts.
            assertThat(deadCount).isEqualTo(3);

        } finally {
            kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        }
    }

    private KafkaConsumer<String, String> createConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-outbox-relay-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        consumer.subscribe(Collections.singletonList("number-portability-events"));
        return consumer;
    }
}
