# Changelog

## 0.3.0 — 2026-07-18

Second saga: `bulk-sim-provisioning`.

- Provision a batch of SIMs; if the failure rate exceeds a configurable threshold, deprovision
  exactly the ICCIDs that succeeded (never the ones that failed). Below the threshold, partial
  success is accepted and failures are just reported.
- `SimProvisioningGateway` interface + `SimulatedSimProvisioningGateway` default (always
  succeeds — a placeholder for a real network element client).
- `ProvisionBatchDelegate` / `CompensateBatchDelegate`, both publishing per-ICCID Kafka events.
- REST API: start a batch, check status.
- `BulkSimProvisioningTest`: all-success, above-threshold rollback (only successful ICCIDs
  deprovisioned), below-threshold partial-success-accepted — gateway mocked per ICCID, no
  randomness.

## 0.2.0 — 2026-07-18

Reconciliation sweep for stuck sagas.

- `StuckSagaReconciliationService`: finds active saga instances sitting in the same activity
  longer than `provisioning.reconciliation.stuck-threshold` (default 15 minutes), publishes a
  `reconciliation.stuck_saga_detected` Kafka event per stuck instance.
- `POST /api/reconciliation/run` — manual/on-demand trigger, returns the stuck sagas found.
- Uses Camunda's `ClockUtil` rather than wall-clock time, consistent with the SLA timeout test's
  approach, so the sweep logic is testable without a real wait.
- Tests: a fresh saga isn't flagged, a saga pushed past the threshold is flagged and its event
  published.

## 0.1.0 — 2026-07-18

First saga: `number-portability-saga`.

- Multi-operator number portability process: validate → notify donor → await response with an
  SLA boundary timer → activate/compensate → (on rejection or timeout) manual review.
- `ValidateRequestDelegate`, `NotifyDonorOperatorDelegate`, `ActivateOnRecipientDelegate`,
  `NotifyCompletionDelegate`, `CompensateProvisioningDelegate` — the last one shared by both the
  rejection and timeout paths, since they're the same business outcome.
- REST API: start a request, submit a donor response (message correlation), query status.
- Kafka event publishing (`donor.notification.requested`, `portability.activated`,
  `portability.completed`, `portability.compensated`) via a swappable
  `PortabilityEventPublisher`.
- `NumberPortabilitySagaTest`: all four paths (accept, reject, SLA timeout via engine clock
  manipulation, invalid input) against the embedded Camunda engine, no Docker.
- `PortabilityEventPublisherIT`: Testcontainers Kafka check that the real producer config and
  JSON envelope are correct.
