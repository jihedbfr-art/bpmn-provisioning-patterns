# bpmn-provisioning-patterns

[![CI](https://github.com/jihedbfr-art/bpmn-provisioning-patterns/actions/workflows/ci.yml/badge.svg)](https://github.com/jihedbfr-art/bpmn-provisioning-patterns/actions)

Most public Camunda examples are pizza orders: order placed, payment charged, pizza delivered,
the end. Real orchestration has none of that tidiness — external systems that don't answer,
SLAs that force a decision anyway, and rollbacks that have to undo work that already happened.
This repo is one executable process built to those constraints instead: a multi-operator number
portability saga, modeled on how it actually works between telecom operators, not a diagram made
up for a slide.

## The process

A subscriber asks to move their number from a donor operator to a recipient operator.
`number-portability-saga.bpmn`:

1. **Validate the request** — msisdn and both operators present, donor and recipient not the
   same operator. Fails loudly (not silently) on bad input.
2. **Notify the donor operator** — publishes a Kafka event. In a real deployment this is where a
   message would cross an inter-operator integration boundary (SOAP/REST gateway, MNP clearing
   house, whatever the market uses); here it's a Kafka topic standing in for that boundary.
3. **Wait for the donor's response — with a hard SLA.** Modeled as an embedded subprocess (a
   message intermediate catch event) with a boundary timer event on it. If the donor answers in
   time, the message wins. If not, the timer fires and the saga takes the same path as an
   explicit rejection — a real regulatory SLA doesn't care *why* the donor didn't answer, only
   that they didn't.
4. **Accepted** → activate on the recipient network, notify completion, done.
   **Rejected or timed out** → compensate (roll back anything provisioned so far) → a human
   **Manual Review** task, because a real rejection usually needs a person to look at it before
   the case is closed, not just an automatic retry.

## Why an embedded subprocess for the timeout, not two separate paths

A boundary timer event has to attach to an activity, not to a bare message catch event, so the
"wait for a message with a timeout" pattern needs the catch event wrapped in a subprocess with
the timer on the subprocess boundary. It's a few more BPMN elements than the naive version, but
it means the timeout and the rejection converge on the exact same compensation task instead of
two copies of the same rollback logic drifting apart over time.

## A second saga: bulk SIM provisioning

`bulk-sim-provisioning.bpmn` is a different shape of the same underlying idea — partial failure
in a batch, and a rule deciding whether that's acceptable or needs undoing. Provision a batch of
SIMs; if the failure rate stays under a threshold, the batch is accepted as-is (the failures get
reported for retry, the successes stand) — if it goes over, the whole batch gets rolled back by
deprovisioning exactly the SIMs that succeeded, not the ones that never provisioned in the first
place.

This one loops the batch inside a single service task instead of modeling each SIM as a BPMN
multi-instance activity. Multi-instance would make each SIM individually visible and resumable in
Cockpit, which is a real advantage for some use cases — but aggregating parallel-instance results
back into one failure-rate decision means fighting Camunda's per-instance variable scoping for a
benefit this particular case doesn't need. Nobody's pausing mid-batch to inspect one SIM; the
batch-level pass/fail is what matters here.

`SimProvisioningGateway` is a placeholder for whatever the real target is — an EIR/HSS API, a
CRM back-office call. The default implementation always succeeds; the tests control failure per
ICCID through a mock, deliberately not through randomness (a test that fails 1 run in 20 is worse
than no test).

## Reconciliation

Event-driven design assumes every message eventually shows up. In practice some don't — an
integration silently drops a callback, a reviewer forgets a manual review task exists — and the
SLA timer alone doesn't catch that, because a saga can sit one step *before* the timer is even
running, or in the manual review task after the timer's already done its job. `POST
/api/reconciliation/run` sweeps every active saga, flags any sitting in the same activity longer
than `provisioning.reconciliation.stuck-threshold` (default 15 minutes — deliberately shorter
than the SLA itself, so ops finds out before a customer does), and publishes a
`reconciliation.stuck_saga_detected` event per stuck instance. In production this is wired to a
schedule (cron, Spring `@Scheduled`, whatever the deployment already uses for batch jobs); it's
exposed as an endpoint here mainly so it's testable and triggerable on demand.

## Stack

Spring Boot 3.2, Camunda 7.23 (embedded engine — matches how this actually gets deployed in
practice: the process engine runs inside the application, not as a separate cluster), Kafka.

A note on Camunda 7: the community edition is no longer receiving new releases — Camunda's
current investment is Camunda 8 (Zeebe), a different architecture (external broker, not
embedded). I used 7 here anyway because the pattern in this repo — sagas, compensation,
timeout-as-rejection, human review — is the point, not the specific engine, and it transfers
directly to Camunda 8 or any other orchestrator. Porting to 8 is on the roadmap.

## Running it

```bash
mvn spring-boot:run
```

Start a portability request:

```bash
curl -X POST localhost:8080/api/portability \
  -H "Content-Type: application/json" \
  -d '{"msisdn":"+21620000000","donorOperator":"Ooredoo","recipientOperator":"Orange"}'
# {"requestId":"...", "processInstanceId":"..."}
```

Submit the donor's response (normally this would be triggered by an inbound event, not curl):

```bash
curl -X POST localhost:8080/api/portability/{requestId}/donor-response \
  -H "Content-Type: application/json" -d '{"decision":"ACCEPTED"}'
```

Check status:

```bash
curl localhost:8080/api/portability/{requestId}
```

If nobody calls `donor-response` before the SLA in `provisioning.sla.donor-response-timeout`
elapses, the saga times out into the same compensation + manual review path as an explicit
rejection.

Start a bulk SIM provisioning batch:

```bash
curl -X POST localhost:8080/api/bulk-provisioning \
  -H "Content-Type: application/json" \
  -d '{"simRequests":[{"iccid":"8921...01","msisdn":"+21620000001"},{"iccid":"8921...02","msisdn":"+21620000002"}],"rollbackThreshold":0.2}'
# {"batchId":"...", "processInstanceId":"..."}
```

`rollbackThreshold` is optional (defaults to `provisioning.bulk-sim.default-rollback-threshold`,
0.2). Check status the same way: `curl localhost:8080/api/bulk-provisioning/{batchId}`.

## Testing

```bash
mvn test      # process tests against the embedded H2 engine — no Docker
mvn verify     # also runs the Testcontainers check against a real Kafka broker
```

`NumberPortabilitySagaTest` drives the saga through Camunda's embedded engine and asserts on the
actual process state — active activity IDs, historic end-activity IDs, task queries — for all
four paths: donor acceptance, donor rejection, SLA timeout (using `ClockUtil` to fast-forward the
engine clock and firing the boundary timer job directly, not a real `Thread.sleep`), and invalid
input never reaching the donor notification step. `PortabilityEventPublisherIT` boots the full
Spring context against a real Kafka broker via Testcontainers and reads back a published event
to confirm the producer config and JSON envelope are actually right.
`StuckSagaReconciliationServiceTest` covers both a fresh saga (not reported) and one pushed past
the stuck threshold via the same clock-manipulation approach as the SLA test.
`BulkSimProvisioningTest` covers all-success, failure-rate-above-threshold (rollback, and only
the successful ICCIDs get deprovisioned), and failure-rate-below-threshold (partial success
accepted, no rollback) — with the gateway mocked to fail specific ICCIDs deterministically rather
than randomly.

## Roadmap

- Port to Camunda 8 / Zeebe as a second, parallel implementation of the same patterns
- Wire the reconciliation sweep to an actual schedule instead of only a manual endpoint

## License

MIT — see [LICENSE](LICENSE).
