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
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ProvisioningApplication.class, properties = {
        "provisioning.outbox.relay.enabled=false",
        "camunda.bpm.job-execution.enabled=false"
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

    @Test
    void testOutboxNormalAndBrokerOutage() throws Exception {
        // --- Setup Consumer ---
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-outbox-relay-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        ));
        consumer.subscribe(Collections.singletonList("number-portability-events"));

        // Case 1: Saga started, outbox written, but NOT published yet
        String req1 = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey("number-portability-saga", req1, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT30M"
        ));

        // Wait to make sure Camunda committed it to DB
        Integer unpublished = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NULL", Integer.class, req1);
        assertThat(unpublished).isEqualTo(1);

        ConsumerRecords<String, String> emptyRecords = consumer.poll(Duration.ofSeconds(2));
        assertThat(emptyRecords.isEmpty()).isTrue(); // Nothing on Kafka yet

        // Case 2: Call publishBatch manually -> Kafka topic receives it
        outboxRelay.publishBatch();

        Integer afterPublish = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", Integer.class, req1);
        assertThat(afterPublish).isEqualTo(1);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertThat(records.isEmpty()).isFalse();
            ConsumerRecord<String, String> rec = records.iterator().next();
            assertThat(rec.key()).isEqualTo(req1);
            JsonNode node = mapper.readTree(rec.value());
            assertThat(node.get("eventType").asText()).isEqualTo("PortabilityRequestedEvent");
        });

        // Case 3: Broker Outage
        // We use pause/unpause to simulate an outage because stop/start would change the mapped port
        // and Spring's KafkaProducer would be permanently unable to reconnect to the new random port.
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();

        String req2 = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey("number-portability-saga", req2, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT30M"
        ));

        Integer unpublished2 = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NULL", Integer.class, req2);
        assertThat(unpublished2).isEqualTo(1);

        // Try to publish while broker is down
        outboxRelay.publishBatch();

        // Should still be unpublished, but attempts > 0
        Map<String, Object> record2 = jdbc.queryForMap("SELECT attempts, last_error FROM portability_outbox WHERE aggregate_id = ?", req2);
        assertThat((Integer) record2.get("attempts")).isGreaterThan(0);
        assertThat(record2.get("last_error")).isNotNull();

        // Restore broker
        kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
        
        // Wait for Kafka to settle
        Thread.sleep(1000);

        outboxRelay.publishBatch();
        
        Integer afterPublish2 = jdbc.queryForObject("SELECT count(*) FROM portability_outbox WHERE aggregate_id = ? AND published_at IS NOT NULL", Integer.class, req2);
        assertThat(afterPublish2).isEqualTo(1);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            assertThat(records.isEmpty()).isFalse();
            // find the record
            boolean found = false;
            for (ConsumerRecord<String, String> r : records) {
                if (req2.equals(r.key())) found = true;
            }
            assertThat(found).isTrue();
        });
        
        consumer.close();
    }
}
