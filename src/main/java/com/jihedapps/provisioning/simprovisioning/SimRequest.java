package com.jihedapps.provisioning.simprovisioning;

/** One SIM to provision as part of a batch. */
public record SimRequest(String iccid, String msisdn) {
}
