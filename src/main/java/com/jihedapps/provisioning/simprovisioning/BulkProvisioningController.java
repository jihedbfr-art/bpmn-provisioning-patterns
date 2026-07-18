package com.jihedapps.provisioning.simprovisioning;

import org.camunda.bpm.engine.HistoryService;
import org.camunda.bpm.engine.RuntimeService;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/bulk-provisioning")
public class BulkProvisioningController {

    private static final String PROCESS_KEY = "bulk-sim-provisioning";

    private final RuntimeService runtimeService;
    private final HistoryService historyService;
    private final double defaultRollbackThreshold;

    public BulkProvisioningController(RuntimeService runtimeService, HistoryService historyService,
                                       @Value("${provisioning.bulk-sim.default-rollback-threshold}") double defaultRollbackThreshold) {
        this.runtimeService = runtimeService;
        this.historyService = historyService;
        this.defaultRollbackThreshold = defaultRollbackThreshold;
    }

    public record BulkProvisioningRequest(List<SimRequest> simRequests, Double rollbackThreshold) {
    }

    @PostMapping
    public ResponseEntity<?> startBatch(@RequestBody BulkProvisioningRequest request) {
        String batchId = UUID.randomUUID().toString();

        List<Map<String, String>> simRequests = request.simRequests().stream()
                .map(r -> {
                    Map<String, String> m = new HashMap<>();
                    m.put("iccid", r.iccid());
                    m.put("msisdn", r.msisdn());
                    return m;
                })
                .collect(Collectors.toList());

        double threshold = request.rollbackThreshold() != null ? request.rollbackThreshold() : defaultRollbackThreshold;

        ProcessInstance instance = runtimeService.startProcessInstanceByKey(PROCESS_KEY, batchId, Map.of(
                "simRequests", simRequests,
                "rollbackThreshold", threshold
        ));

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "batchId", batchId,
                "processInstanceId", instance.getProcessInstanceId()
        ));
    }

    @GetMapping("/{batchId}")
    public ResponseEntity<?> getStatus(@PathVariable String batchId) {
        var historicInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceBusinessKey(batchId)
                .singleResult();
        if (historicInstance == null) {
            return ResponseEntity.notFound().build();
        }

        if (historicInstance.getEndActivityId() == null) {
            return ResponseEntity.ok(Map.of("batchId", batchId, "status", "IN_PROGRESS"));
        }

        String status = "completed".equals(historicInstance.getEndActivityId()) ? "COMPLETED" : "ROLLED_BACK";
        return ResponseEntity.ok(Map.of("batchId", batchId, "status", status));
    }
}
