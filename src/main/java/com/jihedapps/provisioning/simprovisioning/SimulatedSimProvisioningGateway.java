package com.jihedapps.provisioning.simprovisioning;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Default wiring so the app runs out of the box — always succeeds. This is a placeholder, not a
 * design decision: point {@link SimProvisioningGateway} at a real network element client before
 * this touches an actual subscriber base. Kept deliberately dumb rather than adding fake random
 * failures, which would just make local runs flaky for no real benefit.
 */
@Component
public class SimulatedSimProvisioningGateway implements SimProvisioningGateway {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatedSimProvisioningGateway.class);

    @Override
    public boolean provision(String iccid, String msisdn) {
        LOG.info("simulated provisioning of ICCID {} / MSISDN {}", iccid, msisdn);
        return true;
    }

    @Override
    public void deprovision(String iccid) {
        LOG.info("simulated deprovisioning of ICCID {}", iccid);
    }
}
