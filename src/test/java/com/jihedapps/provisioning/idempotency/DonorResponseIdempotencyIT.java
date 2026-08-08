package com.jihedapps.provisioning.idempotency;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.ProvisioningApplication;
import com.jihedapps.provisioning.kafka.DonorResponseEvent;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricVariableInstance;
import org.camunda.bpm.engine.HistoryService;
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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(classes = ProvisioningApplication.class)
@ActiveProfiles("postgres")
class DonorResponseIdempotencyIT {

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
    void testIdempotentConsumer() throws Exception {
        String requestId = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey("number-portability-saga", requestId, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT30M"
        ));

        String eventId = UUID.randomUUID().toString();
        DonorResponseEvent event = new DonorResponseEvent(eventId, requestId, "ACCEPTED", Instant.now());
        String payload = objectMapper.writeValueAsString(event);

        // Publish multiple times
        kafkaTemplate.send("donor-response-events", requestId, payload);
        kafkaTemplate.send("donor-response-events", requestId, payload);
        kafkaTemplate.send("donor-response-events", requestId, payload);

        // Wait for Camunda to process the message and set the variable
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            org.camunda.bpm.engine.history.HistoricProcessInstance hpi = historyService.createHistoricProcessInstanceQuery()
                    .processInstanceBusinessKey(requestId)
                    .singleResult();
            assertThat(hpi).isNotNull();

            HistoricVariableInstance var = historyService.createHistoricVariableInstanceQuery()
                    .processInstanceId(hpi.getId())
                    .variableName("donorDecision")
                    .singleResult();
            assertThat(var).isNotNull();
            assertThat(var.getValue()).isEqualTo("ACCEPTED");
        });

        // Ensure the event is in the processed_events table exactly once
        Integer processedCount = jdbc.queryForObject(
                "SELECT count(*) FROM processed_events WHERE event_id = ?",
                Integer.class, eventId);
        
        assertThat(processedCount).isEqualTo(1);
    }
}
