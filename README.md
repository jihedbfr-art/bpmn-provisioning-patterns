<div align="center">
  <img src="./assets/brand/jihedailabs-logo.svg" alt="JihedAiLabs" width="180"/>
</div>

# BPMN Provisioning Patterns

<div align="center">

**A <a href="https://github.com/jihedbfr-art">JihedAiLabs</a> project** — Distributed process orchestration and saga patterns using Camunda, Spring Boot, and Kafka.

</div>

---

## Overview

`bpmn-provisioning-patterns` provides production-grade orchestration blueprints for resilient distributed systems and telecom/e-commerce provisioning workflows.

## Quick Start

```bash
docker compose up -d
```
Then navigate to Camunda Cockpit: `http://localhost:8080/camunda` (credentials: `demo` / `demo`).

To start a saga:
```bash
curl -X POST http://localhost:8080/api/portability \
  -H "Content-Type: application/json" \
  -d '{"msisdn": "+21620000000", "donorOperator": "Ooredoo", "recipientOperator": "Orange", "donorResponseTimeout": "PT30M"}'
```

To submit a donor response:
```bash
curl -X POST http://localhost:8080/api/portability/{requestId}/donor-response \
  -H "Content-Type: application/json" \
  -d '{"decision": "ACCEPTED"}'
```

## Running it

### With Docker (Postgres, durable)
Run `docker compose up -d`. This boots Kafka (KRaft), PostgreSQL, and the Spring Boot application using the `postgres` profile. This mode is durable: sagas and their timers will survive an application restart.

### Without Docker (H2 in-memory, fastest)
Simply run `mvn spring-boot:run`. The application starts instantly using an in-memory H2 database. Perfect for fast inner-loop development, but all state is lost upon restart.

## The process

This repository models a Multi-Operator Number Portability (MNP) process: a highly regulated telecom workflow with strict SLAs, external asynchronous communication, and compensation logic.

<details>
<summary>Not from telecom? Read this mapping</summary>

Most BPMN examples are pizza orders. This repository solves real distributed systems challenges, but if telecom jargon is unfamiliar, here is the exact E-commerce equivalent:

- `Validate request` → `Validate cart`
- `Notify donor operator` → `Request payment authorization`
- `Await donor response (SLA)` → `Await payment confirmation`
- `Activate on recipient` → `Reserve inventory`
- `Compensate / rollback` → `Refund payment`
- `Manual review` → `Fraud review queue`
- `Bulk SIM batch` → `Bulk order fulfilment`
</details>

## 🔥 Break it

This repository is built to be tested against failures.

| # | Manipulation | Real expected behavior |
|---|---|---|
| 1 | `docker compose stop kafka` then start a saga | The saga **continues** but the notification event is silently lost (logged as error). This is a known dual-write issue to be solved via the outbox pattern. |
| 2 | Start a saga then `docker compose restart app` | The instance and its SLA timer survive the restart (thanks to PostgreSQL). Try this with H2 to see the contrast. |
| 3 | Start a saga and never call `donor-response` | After `provisioning.sla.donor-response-timeout`, it automatically falls back to compensation + manual review, exactly like an explicit rejection. |
| 4 | Start a SIM batch with failure rate > `rollbackThreshold` | The entire batch triggers a rollback, but only the ICCIDs that *actually succeeded* are deprovisioned. |

## Roadmap
- Transactional Outbox pattern to solve the Camunda/Kafka dual-write.
- Idempotent Kafka consumer to handle retries safely.
- Camunda 8 migration.
