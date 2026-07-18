package com.jihedapps.provisioning.delegate;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Reached from two different places in the process — an explicit donor rejection, or the SLA
 * boundary timer firing because the donor never answered. Both are the same business outcome
 * (the portability didn't go through, undo anything provisioned so far) even though they arrive
 * via different BPMN paths, which is why this is one delegate with two incoming sequence flows
 * rather than two copies of the same rollback logic.
 */
@Component("compensateProvisioningDelegate")
public class CompensateProvisioningDelegate implements JavaDelegate {

    private final PortabilityEventPublisher publisher;

    public CompensateProvisioningDelegate(PortabilityEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void execute(DelegateExecution execution) {
        String requestId = execution.getBusinessKey();
        Object donorDecision = execution.getVariable("donorDecision");
        String reason = donorDecision == null ? "SLA_TIMEOUT" : "DONOR_" + donorDecision;

        publisher.publish("portability.compensated", requestId, Map.of(
                "msisdn", execution.getVariable("msisdn"),
                "reason", reason
        ));
    }
}
