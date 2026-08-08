package com.jihedapps.provisioning.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jihedapps.provisioning.domain.DonorDecision;
import com.jihedapps.provisioning.domain.PortabilityRequest;
import com.jihedapps.provisioning.kafka.DonorResponseEvent;
import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.history.HistoricActivityInstance;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
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
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String donorResponseTimeout;
    private final String donorResponseTopic;

    public PortabilityController(RuntimeService runtimeService, 
                                 HistoryService historyService,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 @Value("${provisioning.sla.donor-response-timeout}") String donorResponseTimeout,
                                 @Value("${provisioning.kafka.donor-response-topic}") String donorResponseTopic) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.donorResponseTimeout = donorResponseTimeout;
        this.donorResponseTopic = donorResponseTopic;
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
        
        String eventId = UUID.randomUUID().toString();
        DonorResponseEvent event = new DonorResponseEvent(eventId, requestId, decision.name(), Instant.now());
        
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(donorResponseTopic, requestId, json);
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", "Failed to publish donor response"));
        }

        return ResponseEntity.accepted().body(Map.of(
                "requestId", requestId, 
                "eventId", eventId, 
                "status", "ACCEPTED_FOR_PROCESSING"
        ));
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
