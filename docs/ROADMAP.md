# Roadmap

## Current UI slice: theme toggle and step-dialog removal — `READY_FOR_MANUAL_TEST`

Follow [THEME_AND_DIALOG_REMOVAL.md](THEME_AND_DIALOG_REMOVAL.md): remove the whole StepInspector/INSPECT STEP overlay and its Flow/Customize callers; retain normal Activity/configuration and existing learning/domain/progress behavior; add the far-right accessible persisted light/dark toggle with light default, whole-app dark styling, independent validated storage, and saved preference before visible paint. This overrides older required-overlay/light-only roadmap requirements. Record permitted static checks and fresh source review; no tests added/run, full automatic test gate NOT RUN, browser UI/manual section 15 pending. Prior packet/learning milestones below are historical.

Final typecheck/lint/build pass (exit 0) and the fresh source re-review reports 0 P0/P1/actionable P2. One production server is running on port 3000. [Current review evidence](../.agents/review/THEME_DIALOG_REVIEW.md); manual checks and user acceptance remain open.

## Milestone order

0. Foundation and static shell.
1. Topic, partitions, producer, and offsets.
2. Consumer polling and commits.
3. Consumer groups and assignments.
4. Lag, throughput, timeline, and charts.
5. Rebalance.
6. Delivery semantics scenarios.
7. Guided scenarios, inspector, accessibility, and polish.
8. Performance and final review.

Do not start the next milestone while the current milestone has failing tests or build errors.

## Track 2: cluster behavior

Broker/cluster views, leaders and followers, replication, ISR, broker failure, elections, acknowledgements, retries, idempotence, retention, compaction, and dead-letter patterns.

## Track 3: application behavior

Transactions, exactly-once concepts, heartbeats, session timeout, poll interval, cooperative rebalancing, batching, compression, schema registry concepts, and Kafka Streams concepts.

Tracks 2 and 3 are part of the complete local learning product. They are implemented as deterministic browser simulations and do not require real Kafka infrastructure.

## Track 4: process-flow canvas

- Add `@xyflow/react` and its global stylesheet.
- Project the live Kafka state into stable nodes and labeled edges.
- Connect node and edge selection to the inspector.
- Add pan, zoom, fit view, minimap, node movement, keyboard access, reduced motion, and bounded rendering.
- Keep advanced concept controls available without turning every message into a graph node.
- Add record-level transit projection: bounded tokens travel over `producer-${producerId}-topic`, `topic-partition-${partition}`, `partition-consumer-${partition}-${consumerId}`, and `consumer-commit-${consumerId}` only when matching simulation events occur. The compatibility adapter without a producer list retains `producer-topic`.
- Carry message ID, partition, offset, phase, attempt, and source event ID through the transient token model. Keep tokens out of React Flow nodes, persistent state, and Kafka engine state.
- Add tests and manual checks for exact record routing, duplicate attempts, commit range mapping, token bounds, stable node/edge selection, and reduced-motion behavior.

## Track 5: route experience

- Root owns the shared shell, navigation, App Router pages, configuration, and global app CSS.
- `/` is almost entirely the polished live process-flow canvas.
- `/customize` owns custom actions, topology, producer/consumer configuration, scenarios, metrics, timeline, and advanced concepts. `/` keeps only compact playback/sample/reset controls and a selected inspector.
- `/guide` owns the beginner walkthrough, its send/read/save exercise, and technical explanations.
- Route navigation preserves the same client-side simulation state.
- Use a warm light surface with black, gray, and red state accents throughout.

## Track 6: interview learning and Practice

- Remove opening-canvas noise copy while keeping graph semantics and accessible state labels.
- Preserve the beginner Guide send/read/save exercise and add collapsed concept, interview Q&A, and situation based learning layers.
- Add the `/practice` category hub and seven fixed quiz URLs: `easy`, `middle`, `hard`, `extreme-hard`, `most-common`, `trick`, and `situation`.
- Author exactly 50 four-option questions per category with stable IDs, concept tags, selected-answer feedback, explanations, and progress out of 50.

## Track 7: browser persistence

- Persist the serializable simulator, chart/timeline, flow UI, and Practice progress in versioned browser `localStorage`.
- Hydrate on the client with playback paused and validate or recover from invalid, unavailable, or over-quota storage.
- Keep simulator reset independent from Practice reset and cover reload and route-transition behavior in manual checks.

## Track 8: technical opening workload

- Seed a paused, deterministic reference workload so the opening canvas has meaningful technical state without faking live activity: four partitions, RF3/min ISR2, `acks=all`, three real producers, four consumers, eight order keys, 24 records, and fixed queued, read-pending, processed, committed, and failed states. Keep the default graph compact and place payload/timestamp/error detail behind inspection.
- Keep ordered partition queues and full technical metadata behind inspection: offsets, key/value, timestamps, attempts, group-scoped outcomes, current/committed positions, LEO, lag, ownership, leader, replicas, and ISR.
- Add explicit `Load reference flow`; preserve the empty reset state and the Guide's one-record exercise.
- Keep the record log intact after read, processing success, processing failure, or commit. Add retry attempts for failures and retain exact record identity.

## Track 9: knowledge accuracy and attribution

- Audit all Guide, Advanced, inspector, scenario, and Practice claims against [`docs/KAFKA_KNOWLEDGE_AUDIT.md`](KAFKA_KNOWLEDGE_AUDIT.md) and current Apache Kafka sources.
- Correct the educational `acks=all` model to use current ISR and `min.insync.replicas`, and expose that boundary in the UI.
- Add a shared black/warm-white/gray/red footer with copyright and creator details or an explicit pending placeholder.

## Track 10: causal learning and reliable record transit — `READY_FOR_MANUAL_TEST`

- Repair packet start timing relative to each mount, finite expiry, and stale visual cleanup across pause, reset, navigation, hydration/reload, and background visibility.
- Add five collapsed Guide experiments that require a prediction and compare actual simulator evidence: keyed ordering, replay before commit, commit-before-processing skips, worker scaling/hot keys, and failure/retry/manual commit.
- Keep the first one-record lesson, compact canvas, and existing simulator semantics intact; use [LEARNING_EXPERIMENTS.md](LEARNING_EXPERIMENTS.md) for bounded fixtures and truthful advanced/production boundaries.
- Persist independent bounded learning progress and complete typecheck, lint, tests, production build, browser QA, fresh review, and section 14 manual acceptance. Archives and deployment are outside this slice.

All engineering gates pass: final typecheck, lint, 14 files / 167 tests, build, final source re-review, main/broader/supplemental browser suites, and root image inspection. Packets follow actual edge geometry from original event clocks/deadlines/generations and paint their endpoint before bounded removal. Main [after-fix.json](../.agents/review/artifacts/packet-learning/after-fix.json), final [supplemental mobile/storage evidence](../.agents/review/artifacts/packet-learning/supplemental-browser.json), and [fresh review](../.agents/review/PACKET_LEARNING_REVIEW.md) record the result. Background browser coverage uses an isolated visibility-state override, with native handler behavior covered by hook tests. User acceptance remains unchecked; status is `READY_FOR_MANUAL_TEST`.
