# Implementation Checklist

This is the detailed version of the root [CHECKLIST.md](../CHECKLIST.md). Use it when working without Codex.

For each item: update docs, implement code, add tests, run checks, inspect the diff, and record the result before moving on.

## Required report for every completed item

```text
status:
criteria:
files:
tests:
commands and results:
simplifications:
limitations:
manual checks:
```

## Milestone checkboxes

These retain the original build requirements. Current completion evidence and the exact remaining review/acceptance work are in **Current slice evidence** below; these historical boxes do not indicate missing implementation.

- [ ] Foundation: app shell, strict TypeScript, dependencies, directories, first build.
- [ ] Producer: message creation, partition choice, offsets, LEO, events, metrics.
- [ ] Consumer: assignments, polling, current position, commits, controls.
- [ ] Groups: assignment completeness, idle consumers, independent groups.
- [ ] Lag: formula, per-partition/group totals, throughput, bounded charts/timeline.
- [ ] Rebalance: join/leave, deterministic assignment, transition events.
- [ ] Failures: duplicate and loss demonstrations, explanatory labels.
- [ ] Scenarios: all nine examples, including Order lifecycle, Duplicate processing, and Commit too soon, reset/load, guided learning, and Goal/Action/Look for/Why instructions.
- [ ] Inspector: producer, topic, partition, message, consumer, group, offset details.
- [ ] Cluster track: brokers, replicas, ISR, leader election, acknowledgements, retry/idempotence, retention, compaction, and dead-letter flow.
- [ ] Application track: transactions, exactly-once concepts, heartbeats, timeout/poll behavior, cooperative rebalance, batching/compression, schemas, and Streams flow.
- [ ] Delivery: `npm run build` succeeds and `npm run start` serves the production build locally.
- [ ] Flow canvas: bounded graph projection, stable IDs, node/edge inspectors, movement/reset, responsive viewport, keyboard access, reduced motion, and red/black/white/gray palette.
- [x] Route experience: `/` is a focused chart workspace with Run/Pause, Reset workflow, seven bottom tabs, and hover/focus tooltips; `/guide` owns the send/read/save walkthrough; direct `/customize` visits return 404; navigation preserves client-side simulation state.
- [ ] Quality: accessibility, reduced motion, bounded rendering, no fake post-start data.
- [ ] Gate: lint, tests, build, review, zero P0/P1.
- [ ] Interview Guide: preserve the send/read/save exercise and add collapsed concept, interview Q&A, common-trap, and situation disclosures without mutating simulator state.
- [ ] Practice: add the main-nav tab and `/practice` hub plus `/practice/easy`, `/practice/middle`, `/practice/hard`, `/practice/extreme-hard`, `/practice/most-common`, `/practice/trick`, and `/practice/situation`.
- [ ] Quiz content: provide exactly 50 unique four-option questions, stable IDs, concept tags, explanations, selected-answer feedback, and progress for every category.
- [ ] Persistence: save simulator/chart/timeline/flow UI state and selected Practice category/question/answers in versioned `localStorage`; hydrate client-only with playback paused and recover from storage failure.
- [ ] Opening copy: remove `MESSAGE FLOW`, `Ready to run`, `Browser simulation`, and the separate stage-label overlay while preserving graph semantics and accessibility.
- [ ] Persistence tests: cover question counts/schema, answer/progress reload, simulator/chart reload, independent resets, and opening-copy absence.
- [ ] Record transit: project individual message IDs as at most 24 transient tokens across matching producer/topic/partition/consumer/commit edges; never create message nodes or mutate simulation state.
- [ ] Event selectors: include optional local producer provenance on newly produced events, message ID on poll events, and committed message IDs or exact ranges on commit events; preserve phase, partition, offset, consumer, attempt, and source event ID while leaving older unknown provenance intact.
- [ ] Record transit tests: exact edge routing, duplicate attempts, commit mapping, stable node/edge selection, reduced motion, persistence exclusion, and bounded token rendering.
- [x] Tabbed chart: default `/` shows bounded real-snapshot projections for Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity; details appear in accessible node tooltips and no per-message nodes are created.
- [ ] Multiple producers/workers: model `checkout-api`, `payment-service`, and `shipping-service` as real producers with persisted identities/counters and optional local teaching `producerId` event/message provenance; preserve older records with unknown source; default `consumer-0` through `consumer-3` own P0 through P3 in `analytics-group`.

## Current slice — 2026-10-07: live chart connections and related UI fixes — `READY_FOR_MANUAL_TEST`

- [x] Define bounded causal activity for all seven charts, event/state identity, pause/reset/tab/reduced-motion lifecycle, drag connection geometry, and accessible responsive tooltips in [CHART_LIVE_UI_FIXES.md](CHART_LIVE_UI_FIXES.md).
- [x] Root implements finite chart activity, checkpoint routing, truthful Delivery counts/transitions, offset-card spacing, mobile tabs, and responsive pointer/keyboard tooltip fixes.
- [x] Final typecheck, lint, 18 test files / 193 tests, production build, and independent source re-review pass. Remaining P0/P1/P2 = 0 within this repair scope; see [repair evidence](../.agents/review/CHART_LIVE_UI_REVIEW.md).
- [x] Isolated development-browser checks pass for all seven live charts, dragged connections, pause/reset cleanup, both themes, 320px/390px tabs/tooltips, reduced motion, Guide/Practice smoke, and zero runtime/console errors. The user's browser storage is untouched.
- [ ] Complete applicable manual section 16 and record user acceptance before `COMPLETE`. Physical touch devices, natural background-tab behavior, and production-server browser execution remain unclaimed.

The current user requested fixes and checks, so normal regression verification is appropriate. Historical theme/dialog test prohibitions and `NOT RUN` evidence below apply only to that earlier slice; prior results and historical checklist entries are preserved and do not certify this repair. Start with the linked current contract when older continuation instructions conflict.

## Current slice evidence — 2026-10-05

### Theme toggle and step-dialog removal — `READY_FOR_MANUAL_TEST`

- [x] Docs-first [contract](THEME_AND_DIALOG_REMOVAL.md), US-43/44, criteria, and agent/skill route instructions supersede required StepInspector and light-only behavior.
- [x] Root reports removal of `StepInspector.tsx`, Flow/Customize callers, and `kl-inspector` styles; explicit Customize explanation actions use normal Activity and its embedded `InspectorPanel`. Compact selection/focus, configuration, Guide lessons, and domain/progress data remain required regressions.
- [x] Root reports the native far-right moon/sun toggle, light default, shared app/canvas theme tokens, independent `kafkalab:theme:v1` version-1 JSON preference, synchronous head restoration, caught storage errors, and cross-tab preference handling. Manual UI/first-paint/data-preservation checks remain open.
- [x] Final root typecheck, lint, and production build pass (exit 0), and fresh source re-review reports 0 P0/P1/actionable P2. [Current evidence](../.agents/review/THEME_DIALOG_REVIEW.md). Full automatic test gate is **NOT RUN**; no tests added or run. One obsolete existing route-workspaces test case was adapted in place and remains unrun. Prior 167-test/browser evidence does not certify the change.
- [ ] Complete manual section 15 for dialog absence, light/dark coverage, keyboard/mobile behavior, persisted first paint, unavailable storage, and independent reset/data preservation. Browser UI verification and user acceptance remain pending.

### Historical packet lifecycle and guided experiments — `READY_FOR_MANUAL_TEST`

- [x] Docs-first contract defines mount-relative packet start/expiry, stale-transit invalidation, five progressively disclosed causal experiments, actual state comparisons, independent learning progress, and honest teaching boundaries in [LEARNING_EXPERIMENTS.md](LEARNING_EXPERIMENTS.md).
- [x] Implement packet event-clock/deadline/generation lifecycle, event-aged actual SVG path painting, and an exact endpoint frame with `64ms` cleanup fallback. Actual displacement, finite expiry, 24 cap, and reduced motion pass main browser QA.
- [x] Implement the five prediction/run/observe/explain experiments through existing store/core actions, including a guarded separate explicit commit after retry. Preserve the original one-record Guide path and compact `/`.
- [x] Persist validated prediction/completion in independent `kafkalab:learning:v1` storage, keep resets independent, and correct awaiting-commit wording.
- [x] Root typecheck PASS and full gate PASS: lint, 14 files / 167 tests, production build, recorded in [done-gate.log](../.agents/review/artifacts/packet-learning/done-gate.log).
- [x] Fresh independent re-review confirms all four actionable P2 findings repaired and remaining P0/P1/actionable P2 = 0: complete assignment map/idle IDs, visible failure reason, guarded commit with actual checkpoint advancement, and **Inspect current Flow** wording.
- [x] Final refreshed automated gate/source re-review passes for the frame-painted renderer and endpoint cleanup: typecheck, lint, 14 files / 167 tests, build, zero P0/P1/actionable P2 findings.
- [x] Main production-browser QA passes on single `http://localhost:3000`: same-packet CTM displacement, delayed route, expiry, stale transitions, 24 cap, reduced motion, original Guide, five actual-state lessons, separate retry/manual C0→1, validated learning reload without auto-run/results, independent clearing, 320/390px Guide, 11 routes HTTP 200, and empty error list. Background browser trial uses an isolated visibility-state override; native handler behavior has hook-test coverage.
- [x] Broader [compact regression](../.agents/review/artifacts/packet-learning/compact-regression/browser-evidence.json) passes source routing, inspector/keyboard/retry focus, persisted producer selection, five-route mobile/reduced-motion behavior, and error checks.
- [x] Final refreshed gate/source review and mobile/storage/viewport-image proof pass after the `540px` local comparison-scroll refinement: [supplemental-browser.json](../.agents/review/artifacts/packet-learning/supplemental-browser.json), [fresh review](../.agents/review/PACKET_LEARNING_REVIEW.md). Native ArrowRight scrolls locally at both widths without page overflow; real-quiz reset isolation, invalid/quota-blocked storage recovery, and root image inspection pass. Archives/deployment are outside scope.
- [ ] Complete section 14 of [MANUAL_TEST_CHECKLIST.md](MANUAL_TEST_CHECKLIST.md) and user acceptance before `COMPLETE`.

### Historical compact chart and richer reference workload evidence

- [x] Replace the default detailed partition/event presentation with compact aggregate cards; retain row payload, timestamp, error, attempt, and replica detail behind inspection.
- [x] Seed the paused reference workload with 3 real producers, 4 partitions, 4 consumers, 8 distinct order keys, and 24 records using the exact contract in `docs/PROJECT_SPEC.md`.
- [x] Route real producer identities through produce events and transit tokens; persist producer identity and per-producer counters; create fresh unique toolbar order keys while preserving the Guide's `order-7F3K9` sample; preserve existing Guide/Practice/footer/storage behavior and port `3000` scripts.
- [x] Update focused engine, store, persistence, projection, route interaction, and transit tests. Latest root gate: typecheck PASS; lint PASS; 11 test files / 146 tests PASS; Next production build PASS.
- [x] Isolated production-browser checks and root image inspection pass: compact cards, six inspector rows, record source, retry identity/manual commit, reload, all three source lanes, unique orders, producer selection/addition, Guide, keyboard focus, reduced motion, footer, and 390px/320px layouts. All 11 routes return HTTP 200 on port 3000; no other listener on ports 3001–3017.
- [x] The earlier quota-blocked fresh review was superseded by the historical integrated packet/learning fresh re-review with zero remaining P0/P1/actionable P2 findings.
- [ ] Complete user manual acceptance before `COMPLETE`. The packet/learning milestone reached `READY_FOR_MANUAL_TEST` after its final gate/review/main-browser pass; current theme/dialog verification is separate.

Exact evidence and repair files: [UI_REDESIGN_HANDOFF.md](../.agents/review/UI_REDESIGN_HANDOFF.md). The prior technical-chart milestone had a fresh review with zero unresolved P0/P1 findings and 134 passing tests; that evidence is historical and does not certify the current changes.

## 15. Prior technical opening chart baseline

- [x] Add a bounded ordered row projection for every partition with offset, key, value/event label, timestamp, attempt, selected-group state, and failure reason.
- [x] Expose LEO, current position, committed position, lag, owner, leader, RF, min ISR, and ISR in text. Keep the process canvas as the opening visual focus.
- [x] Add explicit `Load reference flow` behavior that is user-triggered and does not overwrite persisted state during hydration. Keep reset and Guide empty-workflow behavior intact.
- [x] Keep records in partition logs after reads, successful processing, failed processing, and commits. Do not create record nodes in React Flow. Final six-row/pinned-failure/scroll browser evidence passes.
- [x] Keep visual pulses at the `1800ms` cleanup TTL and expose separate `stored`, `checkpoint Σ`, and `commit lag` execution-strip labels; final browser confirmation passes.

## 16. Delivery outcomes and broker policy model

- [x] Extend `DeliveryAttempt` with group-scoped `applicationStatus` (`pending`, `succeeded`, `failed`) and optional `failureReason`; do not add a global outcome to `KafkaMessage`.
- [x] Add a pure engine processing-failure action and `record-processing-failed` event carrying message ID, group, consumer, partition, offset, attempt, and safe reason. The failure action leaves the record stored and commit unchanged; both commit modes stop before the latest failed attempt and emit `offset-commit-blocked`. Current-position commit skips only pending unprocessed work in this lab; retry uses the same message ID with a new attempt.
- [x] Keep `record-processed` separate from `offset-committed`. A commit may checkpoint offsets, but its visual token must not imply payload deletion or business success.
- [x] Add `minInSyncReplicas` to broker-cluster state and seed it at `2` for RF3. Make `acks=all` use current ISR and min ISR; do not require every configured replica to be up.
- [x] Add focused engine/store tests for queued/read/processed/failed/committed/skipped outcomes, retry identity, record retention, commit boundaries, and ISR/min-ISR acknowledgement behavior.

## 17. Knowledge audit and footer

- [x] Review every learner-facing Kafka claim in Guide, Advanced, inspectors, scenarios, and all Practice banks against [`docs/KAFKA_KNOWLEDGE_AUDIT.md`](KAFKA_KNOWLEDGE_AUDIT.md) and direct Apache Kafka 4.3 sources. Record changed claims and links.
- [x] Keep browser, local-memory, educational, and simplification labels visible wherever a surface could be mistaken for broker or wire behavior.
- [x] Add the shared black/warm-white/gray/red footer with copyright and the exact `Creator details pending` placeholder while the public profile is empty, on every route.
- [ ] Complete user manual checks for the reference seed, compact chart/inspector, failure/retry semantics, ISR acknowledgement rule, knowledge wording, and footer. The earlier technical-chart milestone reached `READY_FOR_MANUAL_TEST`; current packet/learning gates are recorded above. Mark `COMPLETE` only after acceptance.
