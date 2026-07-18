package com.jihedapps.provisioning.reconciliation;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricActivityInstance;
import org.camunda.bpm.engine.impl.util.ClockUtil;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The pattern every telecom ops team ends up building sooner or later: pure event-driven design
 * assumes every message eventually arrives, and in practice some don't — a donor operator's
 * integration silently drops a callback, a reviewer forgets a task exists. This doesn't try to
 * fix that; it just makes stuck cases visible on a schedule instead of discovering them when a
 * customer calls in three weeks later asking where their number went.
 *
 * <p>Uses {@link ClockUtil} rather than {@code Instant.now()} so a test can fast-forward the
 * engine clock and get consistent "stuck since" calculations, the same way the SLA timeout test
 * does — this is business logic that should be testable without a real wall-clock wait either.
 */
@Component
public class StuckSagaReconciliationService {

    private static final String PROCESS_KEY = "number-portability-saga";

    private final RuntimeService runtimeService;
    private final HistoryService historyService;
    private final PortabilityEventPublisher publisher;
    private final Duration stuckThreshold;

    public StuckSagaReconciliationService(RuntimeService runtimeService,
                                           HistoryService historyService,
                                           PortabilityEventPublisher publisher,
                                           @Value("${provisioning.reconciliation.stuck-threshold}") String stuckThreshold) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
        this.publisher = publisher;
        this.stuckThreshold = Duration.parse(stuckThreshold);
    }

    public List<StuckSagaReport> reconcileStuckSagas() {
        List<StuckSagaReport> stuck = new ArrayList<>();
        long now = ClockUtil.getCurrentTime().getTime();

        List<ProcessInstance> active = runtimeService.createProcessInstanceQuery()
                .processDefinitionKey(PROCESS_KEY)
                .active()
                .list();

        for (ProcessInstance instance : active) {
            for (String activityId : runtimeService.getActiveActivityIds(instance.getId())) {
                HistoricActivityInstance activity = historyService.createHistoricActivityInstanceQuery()
                        .processInstanceId(instance.getId())
                        .activityId(activityId)
                        .unfinished()
                        .singleResult();

                if (activity == null || activity.getStartTime() == null) {
                    continue;
                }

                Duration elapsed = Duration.ofMillis(now - activity.getStartTime().getTime());
                if (elapsed.compareTo(stuckThreshold) < 0) {
                    continue;
                }

                StuckSagaReport report = new StuckSagaReport(
                        instance.getBusinessKey(), activityId, activity.getActivityName(), elapsed);
                stuck.add(report);

                publisher.publish("reconciliation.stuck_saga_detected", instance.getBusinessKey(), Map.of(
                        "activityId", activityId,
                        "activityName", String.valueOf(activity.getActivityName()),
                        "stuckForSeconds", elapsed.getSeconds()
                ));
            }
        }

        return stuck;
    }
}
