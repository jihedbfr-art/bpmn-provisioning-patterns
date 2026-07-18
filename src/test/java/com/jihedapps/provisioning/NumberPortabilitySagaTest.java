package com.jihedapps.provisioning;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.ManagementService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.TaskService;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.runtime.Job;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.task.Task;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/**
 * Exercises the saga end to end against Camunda's embedded (H2) engine — no Docker, runs on
 * every {@code mvn test}. {@link com.jihedapps.provisioning.kafka.PortabilityEventPublisher} is
 * mocked here on purpose: this test is about the process graph taking the right path, not about
 * Kafka — that's what {@code PortabilityEventPublisherIT} is for.
 */
@SpringBootTest
class NumberPortabilitySagaTest {

    private static final String PROCESS_KEY = "number-portability-saga";

    @Autowired
    private RuntimeService runtimeService;
    @Autowired
    private TaskService taskService;
    @Autowired
    private HistoryService historyService;
    @Autowired
    private ManagementService managementService;

    @MockBean
    private PortabilityEventPublisher publisher;

    @AfterEach
    void resetClock() {
        ClockUtil.reset();
    }

    private Map<String, Object> validRequestVariables(String timeout) {
        return Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", timeout
        );
    }

    @Test
    void donorAcceptanceCompletesThePortability() {
        String requestId = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, validRequestVariables("PT30M"));

        assertThat(isAwaitingDonorResponse(requestId)).isTrue();

        correlateDonorResponse(requestId, "ACCEPTED");

        assertThat(activeProcessInstance(requestId)).isNull();
        assertThat(endActivityId(requestId)).isEqualTo("completed");
        verify(publisher).publish(org.mockito.ArgumentMatchers.eq("portability.activated"),
                org.mockito.ArgumentMatchers.eq(requestId), org.mockito.ArgumentMatchers.any());
    }

    @Test
    void donorRejectionGoesThroughCompensationAndManualReview() {
        String requestId = UUID.randomUUID().toString();
        runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, validRequestVariables("PT30M"));

        correlateDonorResponse(requestId, "REJECTED");

        Task review = taskService.createTaskQuery()
                .processInstanceBusinessKey(requestId)
                .taskDefinitionKey("manualReview")
                .singleResult();
        assertThat(review).as("expected a manual review task after a donor rejection").isNotNull();

        verify(publisher).publish(org.mockito.ArgumentMatchers.eq("portability.compensated"),
                org.mockito.ArgumentMatchers.eq(requestId), org.mockito.ArgumentMatchers.any());

        taskService.complete(review.getId());

        assertThat(activeProcessInstance(requestId)).isNull();
        assertThat(endActivityId(requestId)).isEqualTo("rejected");
    }

    @Test
    void slaTimeoutTakesTheSameCompensationPathAsARejection() {
        String requestId = UUID.randomUUID().toString();
        Date start = new Date();
        ClockUtil.setCurrentTime(start);

        runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, validRequestVariables("PT1S"));
        assertThat(isAwaitingDonorResponse(requestId)).isTrue();

        // move the engine clock past the 1-second SLA and let the boundary timer job fire
        ClockUtil.setCurrentTime(new Date(start.getTime() + 5_000));
        Job timerJob = managementService.createJobQuery()
                .processInstanceId(activeProcessInstance(requestId).getId())
                .timers()
                .singleResult();
        assertThat(timerJob).as("expected the SLA boundary timer job to be scheduled").isNotNull();
        managementService.executeJob(timerJob.getId());

        Task review = taskService.createTaskQuery()
                .processInstanceBusinessKey(requestId)
                .taskDefinitionKey("manualReview")
                .singleResult();
        assertThat(review).as("a timeout should land on the same manual review task as a rejection").isNotNull();

        taskService.complete(review.getId());

        assertThat(endActivityId(requestId)).isEqualTo("rejected");
    }

    @Test
    void invalidRequestNeverReachesTheDonorNotification() {
        String requestId = UUID.randomUUID().toString();

        org.junit.jupiter.api.Assertions.assertThrows(RuntimeException.class, () ->
                runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, Map.of(
                        "msisdn", "",
                        "donorOperator", "Ooredoo",
                        "recipientOperator", "Orange",
                        "donorResponseTimeout", "PT30M"
                )));

        verify(publisher, org.mockito.Mockito.never())
                .publish(org.mockito.ArgumentMatchers.eq("donor.notification.requested"),
                        org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    private boolean isAwaitingDonorResponse(String requestId) {
        ProcessInstance instance = activeProcessInstance(requestId);
        if (instance == null) {
            return false;
        }
        return runtimeService.getActiveActivityIds(instance.getId()).contains("donorResponseReceived");
    }

    private void correlateDonorResponse(String requestId, String decision) {
        runtimeService.createMessageCorrelation("DonorResponseMessage")
                .processInstanceBusinessKey(requestId)
                .setVariable("donorDecision", decision)
                .correlate();
    }

    private ProcessInstance activeProcessInstance(String requestId) {
        return runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(requestId)
                .active()
                .singleResult();
    }

    private String endActivityId(String requestId) {
        var historicInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceBusinessKey(requestId)
                .singleResult();
        return historicInstance.getEndActivityId();
    }
}
