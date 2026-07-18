package com.jihedapps.provisioning.simprovisioning;

import com.jihedapps.provisioning.kafka.PortabilityEventPublisher;
import org.camunda.bpm.engine.delegate.DelegateExecution;
import org.camunda.bpm.engine.delegate.JavaDelegate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Only touches what actually succeeded — the failed ICCIDs were never provisioned, deprovisioning
 * them would be a no-op at best and a confusing call to the real gateway at worst. This is the
 * whole point of tracking {@code provisionedIccids} separately instead of just a failure count.
 */
@Component("compensateBatchDelegate")
public class CompensateBatchDelegate implements JavaDelegate {

    private final SimProvisioningGateway gateway;
    private final PortabilityEventPublisher publisher;

    public CompensateBatchDelegate(SimProvisioningGateway gateway, PortabilityEventPublisher publisher) {
        this.gateway = gateway;
        this.publisher = publisher;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute(DelegateExecution execution) {
        String batchId = execution.getBusinessKey();
        List<String> provisioned = (List<String>) execution.getVariable("provisionedIccids");

        for (String iccid : provisioned) {
            gateway.deprovision(iccid);
            publisher.publish("sim.deprovisioned", batchId, Map.of("iccid", iccid, "reason", "BATCH_ROLLED_BACK"));
        }
    }
}
