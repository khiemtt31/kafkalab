# Contribution and Agent Guide

## Change workflow

Every request follows this sequence:

```text
request
→ classify affected story and criteria
→ improvement-qa.bot checks documentation
→ developer implements within ownership
→ developer runs tests and the done gate
→ orchestrator inspects the diff
→ reviewer checks the result
→ fix P0/P1 findings
→ READY_FOR_MANUAL_TEST
```

The loop repeats after every implementation handoff. An agent must not report `COMPLETE` when a required check fails or a P0/P1 review finding remains.

## Documentation first

When behavior changes, update the relevant user story, specification, and acceptance criterion before changing code. Bug fixes only require documentation changes when the existing documentation is inaccurate or the behavior is intentionally changing.

## Ownership

| Area | Owner |
| --- | --- |
| Coordination, shared files, integration | orchestrator/root |
| Requirements and criteria repair | improvement-qa.bot |
| Route shell, app routes, shared configuration, and global CSS | orchestrator/root |
| Application implementation | developer |
| Pure Kafka engine and scenarios | kafka-domain |
| React Flow graph projection, canvas, node/link visuals, and canvas CSS | visualization |
| Flow chart projections, node tooltips, and tabs | visualization |
| Tests and test utilities | test-engineer |
| Final findings | reviewer |

No two agents edit the same file concurrently. The orchestrator resolves shared-file changes.

## Route contract

The application has these user-facing routes:

- `/` is the focused beginner workspace. Its bottom tabs switch among Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity projections without changing simulation state.
- `/guide` provides the beginner walkthrough and a send/read/save exercise. Its first step clearly states that it resets the workflow and disables automatic commit. Guide experiments use internal store actions and return learners to the chart; there is no Customize route.
- `/practice` and its category routes retain interview practice and learning progress. Direct `/customize` visits must return 404 because that route is removed.

The routes share the same in-memory Zustand simulation during client-side navigation. Route changes must not create a second Kafka state model.

## Educational rules

Core code must explain the Kafka concept it represents and label simplifications with `@Simplification`. The engine must not import React, Next.js, Zustand, or DOM APIs.

The opening chart teaches partition-local ordered logs and group-scoped delivery states. A record can be queued, read, processed successfully, failed with an application reason, committed as a group checkpoint, or skipped by the explicit commit-too-soon lesson. The failure action itself does not advance the committed position; both commit modes stop before the latest failed attempt and emit `offset-commit-blocked` under the lab safety policy. Current-position commit can skip pending unprocessed work, while real Kafka permits a client to commit past failed work. Those states never mean that a topic record was deleted. Use [`../docs/KAFKA_KNOWLEDGE_AUDIT.md`](../docs/KAFKA_KNOWLEDGE_AUDIT.md) and current Apache Kafka documentation when editing Guide, Advanced, inspector, scenario, or Practice copy. Qualify acks, retention, commits, transactions, exactly-once, liveness, and broker/protocol behavior rather than presenting the browser simulation as a real Kafka client.

## Reporting

Every agent reports:

```text
status
summary
files changed
criteria addressed
commands run and results
tests added or updated
simplifications
limitations
manual checks remaining
```

Use `READY_FOR_MANUAL_TEST` only after the automated done gate and review pass.
