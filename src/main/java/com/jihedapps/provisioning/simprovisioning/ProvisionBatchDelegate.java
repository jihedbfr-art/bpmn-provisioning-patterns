package com.jihedapps.provisioning.simprovisioning;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Loops the batch in plain Java rather than modeling each SIM as a BPMN multi-instance activity.
 * Multi-instance would make each SIM individually visible in Cockpit, which is a real advantage
 * — but aggregating per-instance results back into a single pass/fail decision means fighting
 * Camunda's parallel-instance variable scoping for a benefit this use case doesn't need (nobody's
 * pausing to look at one SIM mid-batch). A single service task keeps the failure-rate decision
 * simple and the whole thing easy to reason about and test.
 */
@Component("provisionBatchDelegate")
public class ProvisionBatchDelegate implements JavaDelegate {

    private final SimProvisioningGateway gateway;
    private final PortabilityEventPublisher publisher;

    public ProvisionBatchDelegate(SimProvisioningGateway gateway, PortabilityEventPublisher publisher) {
        this.gateway = gateway;
        this.publisher = publisher;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(DelegateExecution execution) {
        String batchId = execution.getBusinessKey();
        List<Map<String, String>> simRequests = (List<Map<String, String>>) execution.getVariable("simRequests");

        List<String> provisioned = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        for (Map<String, String> request : simRequests) {
            String iccid = request.get("iccid");
            String msisdn = request.get("msisdn");

            boolean success = gateway.provision(iccid, msisdn);
            if (success) {
                provisioned.add(iccid);
                publisher.publish("sim.provisioned", batchId, Map.of("iccid", iccid, "msisdn", msisdn));
            } else {
                failed.add(iccid);
                publisher.publish("sim.provisioning_failed", batchId, Map.of("iccid", iccid, "msisdn", msisdn));
            }
        }

        double failureRate = simRequests.isEmpty() ? 0.0 : (double) failed.size() / simRequests.size();

        execution.setVariable("provisionedIccids", provisioned);
        execution.setVariable("failedIccids", failed);
        execution.setVariable("failureRate", failureRate);
    }
}
