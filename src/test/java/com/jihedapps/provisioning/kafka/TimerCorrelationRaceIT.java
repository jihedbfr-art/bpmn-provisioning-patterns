package com.jihedapps.provisioning.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.ProvisioningApplication;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.history.HistoricProcessInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

@SpringBootTest(classes = ProvisioningApplication.class, properties = {
        "provisioning.outbox.relay.enabled=false"
})
@ActiveProfiles("postgres")
class TimerCorrelationRaceIT {

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
    TaskService taskService;

    @Autowired
    DonorResponseListener listener;

    @Autowired
    ObjectMapper objectMapper;

    @Test
    void shouldNotThrowWhenDirectListenerCalledAfterTimerTrigger() throws Exception {
        // Note: This test calls listener.onDonorResponse(payload) directly to prove the decision branch logic
        // and boundary event correlation. It does not test the full Kafka ingestion pipeline.
        String requestId = UUID.randomUUID().toString();
        
        // Start process with 1 second timeout
        runtimeService.startProcessInstanceByKey("number-portability-saga", requestId, Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT1S"
        ));

        // Wait for timer to trigger (Camunda job executor polls every few seconds)
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            HistoricProcessInstance historicProcessInstance = historyService.createHistoricProcessInstanceQuery()
                    .processInstanceBusinessKey(requestId)
                    .singleResult();
                    
            assertThat(historicProcessInstance).isNotNull();

            // Verify timeout path was taken
            long historicActivityCount = historyService.createHistoricActivityInstanceQuery()
                    .processInstanceId(historicProcessInstance.getId())
                    .activityId("slaTimeout")
                    .finished()
                    .count();
            
            assertThat(historicActivityCount).isGreaterThan(0);
        });

        // Simulate incoming message after timeout
        String eventId = UUID.randomUUID().toString();
        DonorResponseEvent event = new DonorResponseEvent(eventId, requestId, "ACCEPTED", Instant.now());
        String payload = objectMapper.writeValueAsString(event);

        // This should NOT throw MismatchingMessageCorrelationException
        assertDoesNotThrow(() -> listener.onDonorResponse(payload));
    }
}
