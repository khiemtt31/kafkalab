# Build Checklist

Check items in order. Do not start a later milestone while an earlier milestone has failing automated checks.

## Current handoff — 2026-10-07: live chart connections and related UI fixes

status: `READY_FOR_MANUAL_TEST`

Follow [CHART_LIVE_UI_FIXES.md](docs/CHART_LIVE_UI_FIXES.md) for finite causal activity across all seven charts, actual event/state identity, drag-updated connections, lifecycle cleanup, and accessible responsive tooltips. Root completed implementation and verification; exact results and remaining limits are in [CHART_LIVE_UI_REVIEW.md](.agents/review/CHART_LIVE_UI_REVIEW.md).

- [x] Document the bounded implementation brief and acceptance/verification requirements.
- [x] Implement chart activity, checkpoint routing, truthful Delivery counts/transitions, offset-card spacing, mobile tabs, and responsive pointer/keyboard tooltip repairs.
- [x] Final typecheck, lint, 18 test files / 193 tests, production build, and independent source re-review pass; remaining P0/P1/P2 = 0 within this repair scope.
- [x] Isolated development-browser checks pass across all seven live charts, dragged connections, pause/reset cleanup, both themes, 320px/390px tabs/tooltips, reduced motion, Guide/Practice smoke, and no browser errors. See [browser evidence](.agents/review/artifacts/chart-live/evidence.json). Physical touch devices and production-server browser behavior are not claimed.
- [ ] Complete applicable manual section 16 and record user acceptance before `COMPLETE`.

The current user requested fixes and checks; normal regression verification is appropriate. Earlier no-test/`NOT RUN` statements and the historical continuation instructions below apply to the theme/dialog slice only. They do not prohibit this repair's checks or certify it; follow the new contract first and preserve historical evidence.

## Current handoff — 2026-10-05

status: `READY_FOR_MANUAL_TEST`

The whole StepInspector/INSPECT STEP dialog is removed and the far-right shared-header theme toggle is implemented. Final typecheck, lint, build, and fresh source review pass; manual section 15 and user acceptance remain open. Follow [THEME_AND_DIALOG_REMOVAL.md](docs/THEME_AND_DIALOG_REMOVAL.md) and [current review evidence](.agents/review/THEME_DIALOG_REVIEW.md). No tests are added/run; one obsolete existing route-workspaces test case was adapted in place and remains unrun. Full automatic test gate is **NOT RUN**. The prior 167-test/browser result below does not certify this slice.

- [x] Update docs/stories/criteria and agent/skill route instructions before implementation; supersede required-overlay/light-only rules.
- [x] Root reports removal of the entire StepInspector component/Flow/Customize callers and styles; normal Activity details remain available, including explicit explanation actions and embedded `InspectorPanel`.
- [x] Root reports the far-right native moon/sun toggle, light default, app/canvas theme tokens, independent validated version-1 theme storage, synchronous pre-paint bootstrap, and caught storage errors. Manual coverage and data/reset independence remain unchecked.
- [x] Final root typecheck/lint/build pass (exit 0) and fresh source re-review reports 0 P0/P1/actionable P2; full test gate NOT RUN and no browser UI proof claimed.
- [ ] Complete manual section 15 and user acceptance before `COMPLETE`.

### Historical packet/learning evidence

All engineering gates pass for the five guided experiments and finite packet lifecycle: final typecheck, lint, 167 tests / 14 files, production build, fresh source re-review, main/broader/supplemental browser suites, and root image inspection. Remaining P0/P1/actionable P2 findings = 0. Actual packets move along their matching edges, paint the endpoint, and disappear finitely. Final records: [fresh review](.agents/review/PACKET_LEARNING_REVIEW.md), [done-gate.log](.agents/review/artifacts/packet-learning/done-gate.log), [main browser](.agents/review/artifacts/packet-learning/after-fix.json), and [supplemental mobile/storage browser](.agents/review/artifacts/packet-learning/supplemental-browser.json). User acceptance remains open; continue with section 14 of [docs/MANUAL_TEST_CHECKLIST.md](docs/MANUAL_TEST_CHECKLIST.md). Archives and deployment are outside scope.

- [x] Document the packet mount-relative start/expiry requirement and five bounded causal lessons before implementation.
- [x] Implement packet event clocks, finite deadlines, stable generations, stale-transit cleanup, and exact endpoint painting; actual same-packet displacement passes production-browser QA.
- [x] Implement five collapsed guided experiments with predictions, actual before/after evidence, workflow replacement notices, guarded manual retry commit, and independent `kafkalab:learning:v1` progress.
- [x] Preserve the Guide one-record exercise, compact canvas, Practice progress, and educational boundaries in implementation/tests; correct awaiting-commit copy.
- [x] Historical packet/learning typecheck and full lint/tests/build gate pass: [done-gate.log](.agents/review/artifacts/packet-learning/done-gate.log), 14 files / 167 tests PASS.
- [x] Obtain fresh independent re-review: four actionable P2 findings repaired, remaining P0/P1/actionable P2 = 0.
- [x] Final refreshed full gate and source re-review pass after frame-painted rendering and exact endpoint cleanup.
- [x] Main production-browser QA passes: packet motion/lifecycle, 24 cap/reduced motion, original Guide, five lessons, separate retry commit, independent learning progress, 320/390px Guide, 11 routes, and no browser errors. Background trial uses an isolated visibility-state override; hook tests cover native handler behavior.
- [x] Broader [compact-flow regression](.agents/review/artifacts/packet-learning/compact-regression/browser-evidence.json) passes source routing, inspectors/keyboard/retry focus, persisted producer selection, five-route mobile/reduced-motion behavior, and error checks.
- [x] Final refreshed gate/source review, mobile/storage browser, and viewport-image inspection pass after readable comparison columns/local scrolling: [supplemental-browser.json](.agents/review/artifacts/packet-learning/supplemental-browser.json). Native ArrowRight moves the local table; all three reset domains preserve the other stores, invalid/quota-blocked learning storage remains usable, and errors are empty.
- [ ] Have the user complete manual section 14 and record acceptance before `COMPLETE`.

- [x] Seven independently routed quiz banks contain exactly 50 questions each, with four options and balanced answer positions.
- [x] Guide interview disclosures, Order lifecycle example, opening-copy cleanup, versioned persistence and independent resets are implemented.
- [x] Historical compact-flow root gate passed `npm run typecheck`, lint, 11 test files / 146 tests, and the Next production build on 2026-10-05; the later historical 167-test gate is recorded above.
- [x] Final [route/server smoke](.agents/review/artifacts/packet-learning/route-server-smoke.json): all 11 routes HTTP 200, one Next listener on port 3000, none on ports 3001–3017.
- [x] Implement compact partition cards, real producer identity/routing, four-consumer reference workload, and inspector-only details: three producers, four partitions, eight orders, 24 records.
- [x] Production-browser checks and root image inspection pass: inspection, retry, source routing, saved state, unique order keys, Guide exercise, keyboard focus, mobile, reduced motion, and footer.
- [x] Historical packet/learning source re-review and main browser suite pass; the earlier reviewer quota blocker is resolved. That evidence remains separate from the current theme/dialog slice, now `READY_FOR_MANUAL_TEST` with permitted static/source checks only.
- [ ] Complete the user manual checklist and record acceptance before `COMPLETE`.

Exact evidence, screenshots, remaining limitations, and repair files: [UI_REDESIGN_HANDOFF.md](.agents/review/UI_REDESIGN_HANDOFF.md). The numbered build sections below retain the full requirements for future changes; they are not a claim that already implemented features are missing.

## Continue without Codex

1. Read [docs/THEME_AND_DIALOG_REMOVAL.md](docs/THEME_AND_DIALOG_REMOVAL.md) and the current slice in [docs/IMPLEMENTATION_CHECKLIST.md](docs/IMPLEMENTATION_CHECKLIST.md). Historical inspector/167-test/packet-browser evidence does not certify this new slice.
2. The current production build is running at `http://localhost:3000`, with one listener and none on 3001–3017 at handoff. This is server availability, not browser UI proof. Before a later manual switch or rebuild, enter `kafkalab`, run `npm run stop`, verify port 3000 is free with `lsof -nP -iTCP:3000 -sTCP:LISTEN` (no listener output), then use one chosen dev/start mode. **Load reference flow** replaces simulator/layout only and preserves Practice/learning/theme. Archives/deployment are outside scope.
3. Follow section 15 of [docs/MANUAL_TEST_CHECKLIST.md](docs/MANUAL_TEST_CHECKLIST.md), then relevant earlier regressions on normal Activity surfaces. Old StepInspector dialog checks are retired. Record pass/fail and reproduction steps.
4. For a failure, update the affected story/criterion and change the linked implementation file in the handoff. Do not add/run tests for this slice unless explicitly requested. Keep Kafka semantics in `src/core/kafka`, state adaptation in `src/store`, and display behavior in `src/components`.
5. For this slice, root records the permitted static `npm run typecheck`, `npm run lint`, and `npm run build` plus fresh source review. Do not add/run tests or invoke the bundled test-running done gate; mark full automatic test gate NOT RUN. Stop/verify the port before any later user-authorized server-mode switch or rebuild, and record exact results/limits.
6. Use `npm run start` against the passing production build, repeat the repaired browser steps, and check keyboard, reduced motion, mobile behavior, and independent learning progress.
7. Keep the theme/dialog slice `IN_PROGRESS` through implementation and permitted static checks/fresh source review. When those pass, report full automatic test gate NOT RUN and manual UI limits before `READY_FOR_MANUAL_TEST`; use `COMPLETE` only after user acceptance. Historical packet/learning gates remain separate.

## 0. Foundation

- [ ] Create a Next.js + React + strict TypeScript app.
- [ ] Configure Tailwind CSS, Zustand, Recharts, Vitest, and React Testing Library.
- [ ] Create `src/core/kafka`, `src/store`, `src/components`, `src/scenarios`, and `tests`.
- [ ] Keep `src/core/kafka` free of React, Next.js, Zustand, chart, and DOM imports.
- [ ] Add a minimal page that builds.
- [ ] Run `npm run lint`, `npm run test`, and `npm run build`.

## 1. Topic, partitions, and producer

- [ ] Implement messages, topics, partitions, producers, partition selection, offsets, and log-end-offset.
- [ ] Start offsets at zero and keep offsets independent per partition.
- [ ] Route the same key deterministically while documenting the simplified hash.
- [ ] Use deterministic round-robin behavior for unkeyed messages.
- [ ] Show the selected partition and allocated offset in the UI.
- [ ] Add unit tests for offsets and partitioning.

## 2. Consumer

- [ ] Implement polling only from assigned partitions.
- [ ] Track current processing position separately from committed offset.
- [ ] Add consume, pause, resume, commit, and crash controls.
- [ ] Show assignments, current position, committed position, and bounded visible log windows.
- [ ] Test consume and commit behavior.

## 3. Consumer groups

- [ ] Assign each partition to at most one active consumer in a group.
- [ ] Support consumers owning multiple partitions.
- [ ] Show idle consumers when consumers outnumber partitions.
- [ ] Keep offsets independent between groups.
- [ ] Test assignment completeness and group independence.

## 4. Lag, metrics, and timeline

- [ ] Calculate `lag = logEndOffset - committedOffset`.
- [ ] Show per-partition, per-group, and total lag.
- [ ] Show produced and consumed throughput from real simulation state.
- [ ] Emit bounded structured timeline events.
- [ ] Add lag and throughput charts with bounded history.

## 5. Rebalance

- [ ] Add and remove consumers.
- [ ] Trigger deterministic reassignment.
- [ ] Show join, revoke, assignment, and completion events.
- [ ] Pause consumption during the simplified rebalance transition.
- [ ] Test deterministic reassignment.

## 6. Delivery semantics

- [ ] Implement crash-before-commit duplicate processing.
- [ ] Implement commit-before-processing message loss demonstration.
- [ ] Explain that commit strategy affects delivery semantics.
- [ ] Add scenario tests and visible duplicate/loss outcomes.

## 7. Scenarios and guided learning

- [ ] Add producer-to-partition, keyed partitioning, slow consumer, scaling, rebalance, duplicate, and multiple-group scenarios.
- [ ] Add **Commit too soon**: poll without processing, commit the next offset, crash/restart, and show skipped work while the record stays stored.
- [ ] Each scenario has a goal, action, observation, and explanation.
- [ ] Expose those instructions in **Examples → How to try it**; show actual result counts for the loss example.
- [ ] Add reset/load scenario controls.
- [ ] Add inspector details for important objects.

## 8. Complete concept tracks

- [ ] Explore broker leaders, replicas, ISR, broker failure, and leader election.
- [ ] Explore required acknowledgements, retry/idempotence behavior, retention, compaction, and dead-letter flow.
- [ ] Explore transaction boundaries, exactly-once concepts, heartbeats, session timeout, and max poll interval.
- [ ] Explore cooperative rebalance, batching, compression, schema evolution, and Kafka Streams flow.
- [ ] Keep each advanced simplification visible in the inspector or explanation copy.

## 9. Historical packet/learning quality gate

- [x] Run the done gate script: historical packet/learning gate passes.
- [x] Run a fresh review: historical packet/learning re-review passes.
- [x] Resolve all P0 and P1 findings; historical packet/learning actionable P2 findings are also repaired.
- [x] Run `npm run lint`.
- [x] Run `npm run test`: 14 files / 167 tests pass.
- [x] Run `npm run build`.
- [x] Run `npm run start` against the passing production build and load `http://localhost:3000`; final route/server smoke passes.
- [x] Confirm no backend is required; the client-side local simulator works in the verified production app.
- [x] The historical packet/learning slice reached `READY_FOR_MANUAL_TEST` after its final automated gate, fresh source re-review, and main production-browser suite. User acceptance remains open; this does not certify the current theme/dialog change.
- [ ] Have the user complete `docs/MANUAL_TEST_CHECKLIST.md` and record acceptance before marking `COMPLETE`.

## 10. Process-flow canvas

- [ ] Install and document `@xyflow/react`.
- [ ] Render a bounded producers → topic → partition → group → consumer flow with actual producer identities.
- [ ] Use stable node and edge IDs with custom handles and labels.
- [ ] Open the inspector from nodes and edges.
- [ ] Support pan, zoom, fit view, minimap, node movement, reset, keyboard focus, and reduced motion.
- [ ] Keep the canvas concise and use red/black/white/gray styling.
- [ ] Add flow projection and interaction tests.
- [ ] Add bounded record tokens keyed by actual message ID and source event ID; route them through the matching producer/topic/partition/consumer/commit edges.
- [ ] Keep at most 24 visible tokens, with no per-record React Flow nodes and no mutation of Kafka state from visual animation.
- [ ] Expose message identity on poll events and committed record identity/ranges on commit events so selectors cannot animate an unrelated record.
- [ ] Test duplicate delivery as a new attempt for the same record, exact partition/consumer routing, stable node/edge selection during transit, reduced motion, and no token replay after reload.

## 11. Route experience and visual accessibility

- [ ] Keep `/` almost entirely the polished live process-flow visualization.
- [ ] Keep forms, adding/configuration actions, scenarios, metrics, and timeline on `/customize`.
- [ ] Put the beginner walkthrough, send/read/save exercise, and plain-language teaching on `/guide`.
- [ ] Keep only compact playback/sample/reset controls and a selected dismissible inspector on `/`.
- [ ] Preserve the same client-side simulation state while navigating between routes.
- [ ] Use warm light surfaces, black text, gray structure, and red state accents.
- [ ] Use text labels alongside color for moving, waiting, idle, paused, and failed states.
- [ ] Confirm the opening route has no long introduction, dense cards, forms, or metrics dashboard.
- [x] Keep visible records/events/charts/animation bounded; latest group/message outcomes remain available, superseded attempts use the 200-attempt history budget, and the six-row partition window pins the oldest unresolved failure while allowing visible rows to scroll.
- [ ] Seed a paused deterministic reference workload on fresh boot: `orders`, four partitions, three real producers, three educational brokers, RF3, min ISR2, `acks=all`, `analytics-group`, four consumers, eight nonsequential order keys, and 24 records with queued, read-pending, processed, committed, and failed states. Keep reset and Guide exercise state empty and explicit.

## 12. Interview Guide and Practice

- [ ] Preserve the existing Guide send/read/save exercise exactly, then add collapsed Kafka concepts, interview Q&A, common traps, and situation diagnosis.
- [ ] Add `Practice` to main navigation and build `/practice` plus seven category routes: `easy`, `middle`, `hard`, `extreme-hard`, `most-common`, `trick`, and `situation`.
- [ ] Ensure every category has exactly 50 four-option multiple-choice questions with stable IDs, concept tags, explanations, selected-answer feedback, and progress out of 50.
- [ ] Persist selected category, selected question, and all answers; keep quiz reset independent from simulator reset.

## 13. Opening copy and local persistence

- [ ] Remove the opening Flow `MESSAGE FLOW` strip, `Ready to run`, `Browser simulation`, and separate numbered/stage-label overlay while retaining graph and accessible state semantics.
- [ ] Persist serializable simulator, chart/timeline, and flow UI state in versioned `localStorage`; hydrate client-only with playback paused.
- [ ] Validate stored data and recover from unavailable, invalid, or quota-exceeded storage without blocking the app.
- [ ] Add automated and manual coverage for quiz data, answer/progress reload, simulator/chart reload, independent resets, opening copy, keyboard access, mobile, and reduced motion.

## 14. Individual record transit

- [ ] Produce a record and show one bounded token through Producer → Topic → its allocated Partition.
- [ ] Poll that record and show it on the actual Partition → Consumer edge for the selected group.
- [ ] Commit that record and show it on the actual Consumer → Commit edge only when its offset is included in the commit event.
- [ ] Keep tokens transient, capped at 24, keyed by message/event identity, excluded from React Flow nodes and localStorage, and safe under reduced motion.
- [ ] Verify duplicate attempts, commit ranges, stale metadata, node/edge selection, reload, and accessibility with the updated test plan.

## 15. Technical opening chart and reference workload

- [x] Replace the default row/event-heavy chart with compact partition cards showing order count, record count, unread/backlog, success/failure, and C/P/LEO; expose full rows/payload/timestamps/errors/replicas only in inspectors and detailed Activity.
- [x] Model the exact three-producer/four-partition/four-consumer/eight-order/24-record paused reference workload in `docs/PROJECT_SPEC.md`; preserve failure, retry, commit, and record-retention semantics.
- [x] Route newly produced events and transit tokens from optional local teaching producer IDs; persist producer identity and per-producer counters; keep automatic ticks round-robin across existing producers; preserve unknown provenance on older records and generate fresh unique toolbar order keys while Guide keeps `order-7F3K9`.
- [x] Confirm compact cards and inspector-only details with production-browser checks and root image inspection.
- [x] Fresh independent review is complete for the historical packet/learning integrated slice.
- [ ] Obtain user manual acceptance for this slice.

## 16. Processing outcome and acknowledgement semantics

- [x] Add group-scoped delivery application status and an explicit processing-failure event/action with retry identity and failure reason.
- [x] Preserve topic records across read, success, failure, and commit. The failure action leaves the commit unchanged; both commit modes stop before the latest failed attempt and emit `offset-commit-blocked`. Current-position commit can skip only pending unprocessed work in this lab. Keep commit as a group checkpoint, not a deletion or business-success signal; real Kafka permits explicit commits past failed work.
- [x] Keep visual activity pulses transient with the `1800ms` cleanup TTL and label the opening execution strip `stored`, `checkpoint Σ`, and `commit lag` separately.
- [x] Add `minInSyncReplicas` to the cluster model and make `acks=all` depend on current ISR and min ISR rather than full replication factor.
- [x] Add tests for all delivery states, retry, retention, commit boundaries, and ISR/min-ISR behavior.

## 17. Knowledge audit and footer

- [x] Complete `docs/KAFKA_KNOWLEDGE_AUDIT.md` against Guide, Advanced, inspectors, scenarios, and all Practice banks; record direct Kafka 4.3 source links and changed claims.
- [x] Add the shared black/warm-white/gray/red footer with copyright and the `Creator details pending` placeholder while `src/config/site-profile.ts` remains an empty public profile.
- [x] Historical technical-chart automated gate and fresh review passed with zero P0/P1 findings; current packet/learning review status is at the top of this checklist.
- [x] Historical technical-chart browser rerun and image review passed; that prior milestone reached `READY_FOR_MANUAL_TEST`.
- [ ] Complete the user manual sections before marking `COMPLETE`; collect creator profile details before publishing footer identity.
