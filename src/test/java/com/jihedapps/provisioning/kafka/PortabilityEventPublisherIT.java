package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The saga test proves the process graph is right using a mocked publisher. This proves the
 * publisher itself — the real {@link KafkaTemplate} config, the JSON envelope shape — actually
 * gets a message onto a real broker, against the full Spring context wired the way production is.
 */
@SpringBootTest
@Testcontainers
class PortabilityEventPublisherIT {

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private PortabilityEventPublisher publisher;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("number-portability-events"));
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    void publishedEventIsReadableFromKafkaWithTheExpectedEnvelope() throws Exception {
        String requestId = "req-" + UUID.randomUUID();
        publisher.publish("donor.notification.requested", requestId, Map.of(
                "msisdn", "+21620000000", "donorOperator", "Ooredoo"));

        ConsumerRecords<String, String> records = pollUntilNotEmpty(consumer, Duration.ofSeconds(20));
        List<ConsumerRecord<String, String>> received = toList(records);

        assertThat(received).hasSize(1);
        assertThat(received.get(0).key()).isEqualTo(requestId);

        JsonNode envelope = new ObjectMapper().readTree(received.get(0).value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("donor.notification.requested");
        assertThat(envelope.get("requestId").asText()).isEqualTo(requestId);
        assertThat(envelope.get("payload").get("donorOperator").asText()).isEqualTo("Ooredoo");
    }

    private List<ConsumerRecord<String, String>> toList(ConsumerRecords<String, String> records) {
        var list = new java.util.ArrayList<ConsumerRecord<String, String>>();
        records.records("number-portability-events").forEach(list::add);
        return list;
    }

    private ConsumerRecords<String, String> pollUntilNotEmpty(
            KafkaConsumer<String, String> consumer, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            if (!records.isEmpty()) {
                return records;
            }
        }
        throw new AssertionError("no records received within " + timeout);
    }
}
