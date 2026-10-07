---
name: kafka-domain
description: Implement or review the pure TypeScript Kafka behavior simulator, including partitions, offsets, consumers, groups, lag, rebalance, and delivery scenarios.
---

Keep the engine independent of React, Next.js, Zustand, charts, and DOM APIs. Preserve partition-local monotonic offsets, committed-versus-current positions, group ownership, independent group offsets, and `lag = logEndOffset - committedOffset`.

Use educational comments from `docs/PROJECT_SPEC.md`; mark deviations from real Kafka with `@Simplification`. Add invariant tests for every semantic change and run the done gate before handoff.
