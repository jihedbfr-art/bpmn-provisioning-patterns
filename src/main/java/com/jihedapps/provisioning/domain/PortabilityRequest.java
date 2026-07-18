package com.jihedapps.provisioning.domain;

/** What a caller submits to kick off a number portability saga. */
public record PortabilityRequest(String msisdn, String donorOperator, String recipientOperator) {
}
