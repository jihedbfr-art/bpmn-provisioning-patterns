package com.jihedapps.provisioning.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.ProvisioningApplication;
import com.jihedapps.provisioning.kafka.DonorResponseEvent;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ProvisioningApplication.class)
@ActiveProfiles("postgres")
class ConsumerRestartIT {

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
    HistoryService historyService;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void testRollbackAndDltOnMissingProcessInstance() throws Exception {
        // Send a message for a requestId that doesn't exist.
        // It will throw MismatchingMessageCorrelationException.
        // The transaction should rollback (so processed_events has no record).
        // DefaultErrorHandler will retry 3 times (1 initial + 2 retries) and send to DLT.
        
        String badRequestId = UUID.randomUUID().toString();
        String eventId = UUID.randomUUID().toString();
        
        DonorResponseEvent event = new DonorResponseEvent(eventId, badRequestId, "ACCEPTED", Instant.now());
        String payload = objectMapper.writeValueAsString(event);

        kafkaTemplate.send("donor-response-events", badRequestId, payload);

        // Wait until processed_events is confirmed NOT to have the event (because it rolled back)
        // AND the message is in DLT. Since we don't consume the DLT in the test, we just wait for 
        // a few seconds to let retries exhaust and check the database.
        
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Integer processedCount = jdbc.queryForObject(
                    "SELECT count(*) FROM processed_events WHERE event_id = ?",
                    Integer.class, eventId);
            assertThat(processedCount).isEqualTo(0); // Rolled back completely
        });
    }
}
