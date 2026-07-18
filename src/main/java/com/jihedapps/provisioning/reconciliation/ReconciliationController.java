package com.jihedapps.provisioning.reconciliation;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * Manual trigger for the reconciliation sweep. A real deployment would also run this on a
 * schedule (Spring's {@code @Scheduled}), but exposing it as an endpoint too means ops can run it
 * on demand — "did anything just get stuck" shouldn't require waiting for the next cron tick.
 */
@RestController
@RequestMapping("/api/reconciliation")
public class ReconciliationController {

    private final StuckSagaReconciliationService reconciliationService;

    public ReconciliationController(StuckSagaReconciliationService reconciliationService) {
        this.reconciliationService = reconciliationService;
    }

    @PostMapping("/run")
    public Map<String, Object> run() {
        List<StuckSagaReport> stuck = reconciliationService.reconcileStuckSagas();
        return Map.of("stuckCount", stuck.size(), "sagas", stuck);
    }
}
