package com.jihedapps.provisioning.reconciliation;

import java.time.Duration;

/** One saga instance found sitting in the same activity longer than the configured threshold. */
public record StuckSagaReport(String requestId, String activityId, String activityName, Duration stuckFor) {
}
