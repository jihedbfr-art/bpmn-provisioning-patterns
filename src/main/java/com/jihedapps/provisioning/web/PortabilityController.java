package com.jihedapps.provisioning.web;

import com.jihedapps.provisioning.domain.DonorDecision;
import com.jihedapps.provisioning.domain.PortabilityRequest;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricActivityInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/portability")
public class PortabilityController {

    private static final String PROCESS_KEY = "number-portability-saga";

    private final RuntimeService runtimeService;
    private final HistoryService historyService;
    private final String donorResponseTimeout;

    public PortabilityController(RuntimeService runtimeService, HistoryService historyService,
                                  @Value("${provisioning.sla.donor-response-timeout}") String donorResponseTimeout) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
        this.donorResponseTimeout = donorResponseTimeout;
    }

    @PostMapping
    public ResponseEntity<?> startPortability(@RequestBody PortabilityRequest request) {
        String requestId = UUID.randomUUID().toString();
        try {
            ProcessInstance instance = runtimeService.startProcessInstanceByKey(PROCESS_KEY, requestId, Map.of(
                    "msisdn", request.msisdn(),
                    "donorOperator", request.donorOperator(),
                    "recipientOperator", request.recipientOperator(),
                    "donorResponseTimeout", donorResponseTimeout
            ));
            return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                    "requestId", requestId,
                    "processInstanceId", instance.getProcessInstanceId()
            ));
        } catch (IllegalArgumentException | org.camunda.bpm.engine.ProcessEngineException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/{requestId}/donor-response")
    public ResponseEntity<?> submitDonorResponse(@PathVariable String requestId,
                                                  @RequestBody Map<String, String> body) {
        DonorDecision decision;
        try {
            decision = DonorDecision.valueOf(body.get("decision"));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", "decision must be ACCEPTED or REJECTED"));
        }

        long correlated = runtimeService.createMessageCorrelation("DonorResponseMessage")
                .processInstanceBusinessKey(requestId)
                .setVariable("donorDecision", decision.name())
                .correlateAllWithResult()
                .size();

        if (correlated == 0) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "no process instance awaiting a donor response for " + requestId));
        }
        return ResponseEntity.ok(Map.of("requestId", requestId, "donorDecision", decision.name()));
    }

    @GetMapping("/{requestId}")
    public ResponseEntity<?> getStatus(@PathVariable String requestId) {
        List<ProcessInstance> active = runtimeService.createProcessInstanceQuery()
                .processInstanceBusinessKey(requestId)
                .active()
                .list();

        if (!active.isEmpty()) {
            List<String> currentActivities = runtimeService.getActiveActivityIds(active.get(0).getId());
            return ResponseEntity.ok(Map.of(
                    "requestId", requestId,
                    "status", "IN_PROGRESS",
                    "currentActivities", currentActivities
            ));
        }

        var historicInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceBusinessKey(requestId)
                .singleResult();
        if (historicInstance == null) {
            return ResponseEntity.notFound().build();
        }

        Optional<HistoricActivityInstance> lastEndActivity = historyService.createHistoricActivityInstanceQuery()
                .processInstanceId(historicInstance.getId())
                .activityType("endEvent")
                .orderByHistoricActivityInstanceEndTime().desc()
                .list().stream().findFirst();

        if (lastEndActivity.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        String status = "completed".equals(lastEndActivity.get().getActivityId()) ? "COMPLETED" : "REJECTED";
        return ResponseEntity.ok(Map.of("requestId", requestId, "status", status));
    }
}
