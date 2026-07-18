# Changelog

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
