package com.jihedapps.provisioning.reconciliation;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@SpringBootTest
class StuckSagaReconciliationServiceTest {

    private static final String PROCESS_KEY = "number-portability-saga";

    @Autowired
    private RuntimeService runtimeService;
    @Autowired
    private StuckSagaReconciliationService reconciliationService;

    @MockBean
    private PortabilityEventPublisher publisher;

    @AfterEach
    void resetClock() {
        ClockUtil.reset();
    }

    private Map<String, Object> variables() {
        return Map.of(
                "msisdn", "+21620000000",
                "donorOperator", "Ooredoo",
                "recipientOperator", "Orange",
                "donorResponseTimeout", "PT2H"
        );
    }

    @Test
    void aFreshSagaIsNotReportedAsStuck() {
        String requestId = UUID.randomUUID().toString();
        Date now = new Date();
        ClockUtil.setCurrentTime(now);

        runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, variables());

        List<StuckSagaReport> stuck = reconciliationService.reconcileStuckSagas();

        assertThat(stuck).noneMatch(report -> report.requestId().equals(requestId));
    }

    @Test
    void aSagaSittingPastTheThresholdIsReportedAndPublished() {
        String requestId = UUID.randomUUID().toString();
        Date start = new Date();
        ClockUtil.setCurrentTime(start);

        runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, variables());

        // stuck-threshold in application.yml (test) is PT15M — move well past it without
        // touching the SLA timeout (PT2H above), so this is purely "nobody's answered yet",
        // not the boundary timer firing
        ClockUtil.setCurrentTime(new Date(start.getTime() + java.time.Duration.ofMinutes(20).toMillis()));

        List<StuckSagaReport> stuck = reconciliationService.reconcileStuckSagas();

        StuckSagaReport report = stuck.stream()
                .filter(r -> r.requestId().equals(requestId))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected " + requestId + " to be reported as stuck"));

        assertThat(report.activityId()).isEqualTo("donorResponseReceived");
        assertThat(report.stuckFor().toMinutes()).isGreaterThanOrEqualTo(20);

        verify(publisher).publish(
                org.mockito.ArgumentMatchers.eq("reconciliation.stuck_saga_detected"),
                org.mockito.ArgumentMatchers.eq(requestId),
                org.mockito.ArgumentMatchers.any());
    }
}
