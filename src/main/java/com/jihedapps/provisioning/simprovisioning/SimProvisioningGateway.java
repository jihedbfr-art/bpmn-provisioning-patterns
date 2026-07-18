package com.jihedapps.provisioning.simprovisioning;

/**
 * Stands in for whatever the real target is — an EIR/HSS API, a CRM back-office call, the kind
 * of thing "create and provision the SIM network in bulk from the back-office" actually meant in
 * practice. Kept as an interface so the process tests can control exactly which ICCIDs fail
 * without relying on randomness (a flaky test that fails 1-in-20 runs is worse than no test).
 */
public interface SimProvisioningGateway {

    boolean provision(String iccid, String msisdn);

    void deprovision(String iccid);
}
