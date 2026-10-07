# UI redesign handoff

status: `READY_FOR_MANUAL_TEST`

## Current handoff: remove INSPECT STEP and add light/dark appearance

The explicit current request removes the whole StepInspector component/overlay and its Flow/Customize callers; normal Customize Activity details, configuration, Guide experiments, and simulator/Practice/learning state remain. The far-right shared-header native toggle defaults to light, applies charcoal/gray/red dark styling app-wide, stores a validated independent preference before visible paint, and never resets domain/progress state. See [THEME_AND_DIALOG_REMOVAL.md](../../docs/THEME_AND_DIALOG_REMOVAL.md).

- [x] Update current docs/stories/criteria and agent/skill routing to supersede required-overlay/light-only behavior.
- [x] Root reports complete removal of the component, both callers, and `kl-inspector` styles. Explicit Customize explanations use normal Activity and embedded `InspectorPanel`. The shared native moon/sun toggle uses independent validated version-1 theme JSON, synchronous head restoration, caught storage errors, cross-tab handling, and app/canvas styling tokens.
- [x] Final root typecheck/lint/build pass (exit 0); fresh source re-review reports 0 P0/P1/actionable P2. [Current evidence](THEME_DIALOG_REVIEW.md). Full automatic test gate is NOT RUN; no tests added/run. One obsolete existing route-workspaces test case was adapted in place and remains unrun. No current browser UI evidence is claimed. One production server is ready on port 3000, with no 3001–3017 listeners at handoff.
- [ ] Complete [manual section 15](../../docs/MANUAL_TEST_CHECKLIST.md#15-remove-inspect-step-and-choose-light-or-dark--current-slice) and user acceptance. Do not use prior 167-test/overlay/browser evidence as current proof.

The older material below records historical packet/learning and chart behavior. StepInspector-based checks are superseded; normal Activity detail/data behavior remains in scope. No browser/server/archive/deployment operations belong to this docs workstream.

Updated: 2026-10-05 (UTC+7). The historical packet/learning evidence is below. Older compact-flow, redesign, Practice, persistence, technical-chart, and transit evidence is retained as historical context. User manual acceptance remains open; archives and deployment were not performed.

## Historical implementation handoff: packet lifecycle and guided experiments

All engineering gates pass for the five guided experiments and finite packet lifecycle: final root typecheck, lint, all 167 tests / 14 files, production build, fresh final source re-review, main/broader/supplemental browser suites, and root image inspection. Actual packets move along matching SVG edges, paint the exact destination, and disappear finitely. Final mobile tables scroll locally with keyboard/touch access and no page overflow; independent reset and invalid/quota-blocked storage trials pass. Formal status is `READY_FOR_MANUAL_TEST`. User acceptance remains unchecked and is required before `COMPLETE`. Full current evidence is in [PACKET_LEARNING_REVIEW.md](PACKET_LEARNING_REVIEW.md).

### Historical packet/learning verification — 2026-10-05

| Check | Result |
| --- | --- |
| Typecheck | PASS reported by root: `npm run typecheck`. |
| Full done gate | PASS: `../.agents/scripts/run-done-gate.sh .`; lint, 14 test files / 167 tests, Next production build. Exact evidence: [done-gate.log](artifacts/packet-learning/done-gate.log). |
| Fresh independent re-review | PASS: [PACKET_LEARNING_REVIEW.md](PACKET_LEARNING_REVIEW.md) includes final frame-painted rendering, exact endpoint/cleanup, all four P2 repairs, final scroll-region correction, and remaining P0/P1/actionable P2 = 0. The earlier reviewer quota blocker is resolved. |
| Latest packet-rendering repair | PASS: final full gate/source review, including six focused packet transform/lifecycle/endpoint tests and one exact destination frame plus `64ms` bounded cleanup fallback. |
| Main production browser | PASS on single `http://localhost:3000`: actual same-packet CTM displacement, delayed routed segment, finite expiry, pause/route/reload/reset generation, 24 cap, reduced motion, original Guide, all five actual-state lessons, failure retry/manual C0→1, progress reload without auto-run/results, independent clearing, all 11 routes HTTP 200, and empty error list. [after-fix.json](artifacts/packet-learning/after-fix.json) is final passing evidence. |
| Background coverage boundary | Browser PASS uses an isolated `document.visibilityState` override plus `visibilitychange` dispatch. Native visibility-handler behavior is covered by hook tests; natural tab-backgrounding remains user manual acceptance. |
| Guide/mobile/root image review | PASS: desktop Guide/Flow and final 320px comparison before/after horizontal scrolling inspected; moving packets verified. [Comparison](artifacts/packet-learning/guide-evidence-viewport-320.png), [scrolled comparison](artifacts/packet-learning/guide-evidence-scrolled-320.png), [moving packet](artifacts/packet-learning/packet-moving.png). Guide at 320px/390px retains readable evidence, footer/predictions, and no page overflow. |
| Broader compact-flow regression | PASS: [browser-evidence.json](artifacts/packet-learning/compact-regression/browser-evidence.json) verifies source routing across actual finite transits, inspector/retry Enter/Space/Escape focus, persisted producer selection, five routes at 320/390px, reduced motion, and no browser errors. |
| Final mobile/storage refinement | PASS: [supplemental-browser.json](artifacts/packet-learning/supplemental-browser.json) records native ArrowRight movement of 40px at both mobile widths, `540px` tables with local ranges 232px/302px and document widths 390px/320px, independent resets using a real answered quiz, ignored invalid entries, in-memory lesson Run during blocked/quota-failed writes, and zero errors. Final refreshed gate and source re-review pass after the scoped `min-width: 0` scroll-region correction. |
| Local server continuation | Final [route-server-smoke.json](artifacts/packet-learning/route-server-smoke.json) confirms all 11 routes HTTP 200, exactly one Next production listener on port 3000, and none on 3001–3017. Open the existing `http://localhost:3000` app for manual checks. Before switching dev/start or rebuilding, run `npm run stop` from the nested app, verify port 3000 is free, then start the chosen mode. |
| User manual acceptance | Pending: follow [MANUAL_TEST_CHECKLIST.md](../../docs/MANUAL_TEST_CHECKLIST.md), section 14, then relevant earlier regression items. |

### Implemented learning behavior

- `/guide` retains the original one-record `order-7F3K9` / `order-created` send/read/save path. Five initially collapsed experiments require a prediction before explicit Run, then show captured Before/After evidence, explanation, interview follow-up, and local-simulation boundaries: Same key, ordered lane; Success, crash, replay; Save too early, skip work; More workers, one hot partition; Retry the record, then save.
- Runs use shared store/core actions and replace the workflow only after the visible Run notice. Captured evidence uses actual records, positions, attempts, assignments, and events; it is not a promise that the live workflow still matches that earlier run.
- Failure retry keeps the stored record identity/count and does not advance manual commit. The separate commit step requires the same workflow/group/owner/attempt and a valid current read position; success is shown only after the actual committed cursor advances.
- Independent `kafkalab:learning:v1` storage validates stable experiment IDs, prediction choices, and completion. It restores only learning progress; it never runs lessons or restores captured results as live state. Learning reset, simulator reset, and Practice reset preserve each other's data. Completion records participation and does not claim production mastery.
- Comparison tables keep a `540px` readable minimum inside local horizontal scrolling, with a labelled keyboard-focusable container and visible mobile swipe hint; mobile touch/keyboard access reaches every evidence column without page overflow.
- The four P2 repairs confirmed by fresh re-review are the complete assignment map and idle IDs, visible failure reason, guarded retry commit with owner/restart regression coverage and actual checkpoint advancement, and **Inspect current Flow** wording.
- Awaiting commit now describes unsaved group progress. Producer/topic/partition and consumer/commit animations remain append/checkpoint teaching paths; advanced policy flags do not imply executed production broker protocols.

### Packet implementation and final browser proof

`useFlowActivity.ts` gives each fresh visual event a browser presentation clock and generation; old events are watermarked so mount/reload/navigation/visibility cleanup cannot replay stale activity. `flow-transit.ts` carries that clock/generation into correctly routed message tokens. `FlowPacket.tsx` uses event-aged `requestAnimationFrame` to paint an SVG transform at `getPointAtLength` along the actual edge path, with a delayed second segment, original event deadline, and remaining-life handling. It paints the exact destination for one frame, then removes the packet, with a `64ms` fallback if frames stop. Token-object/path rerenders update geometry without granting a fresh lifetime. Cleanup cancels frames, deadlines, and settle timers. The `1800ms` event TTL, 24-token limit, reduced motion, and transient-only persistence boundary remain required.

Final [after-fix.json](artifacts/packet-learning/after-fix.json) proves actual displacement of the same packet in both produce and delayed route phases, then finite expiry; it is the passing main browser result. Earlier native-renderer failures are preserved only as diagnostic history in [packet-debug.json](artifacts/packet-learning/packet-debug.json), the native-probe images, and [before-fix.json](artifacts/packet-learning/before-fix.json). Six final packet tests cover late mounting, delayed routing with exact endpoint/removal, no lifetime restart on token/path updates, remaining-life remount, finite reduced-motion/preference behavior, and unmount timer cleanup. Browser state remains an educational local simulation, not production Kafka execution.

### Historical packet/learning slice gate

- [x] Docs/spec/stories/criteria and five experiment fixtures reconciled in [LEARNING_EXPERIMENTS.md](../../docs/LEARNING_EXPERIMENTS.md).
- [x] Learning implementation, independent validated progress, original Guide behavior, and checkpoint copy are present and covered by current tests/review.
- [x] Packet event-clock/deadline/generation implementation and focused regression tests are present.
- [x] Typecheck, lint, 14 files / 167 tests, and production build pass.
- [x] Fresh independent re-review confirms zero remaining P0/P1/actionable P2 findings after the four repairs.
- [x] Final refreshed full gate and source re-review pass for frame-painted rendering and exact endpoint cleanup.
- [x] Main production-browser suite passes actual packet motion/lifecycle, five lesson results, progress persistence, original Guide, 24 cap/reduced motion, 320/390px Guide, 11 routes, and error checks.
- [x] Set `READY_FOR_MANUAL_TEST` after final code/gate/review/main-browser checks.
- [x] Broader compact regression passes routing, inspectors, keyboard/retry focus, persisted producer selection, five-route mobile/reduced-motion behavior, and error checks.
- [x] Final refreshed gate/source review and supplemental mobile/storage/viewport-image proof pass after the comparison-table refinement.
- [ ] Record user manual acceptance before `COMPLETE`.

## Historical implementation handoff: compact partition chart and richer reference workload

This earlier slice was implemented and passed its root gate, production-browser checks, route smoke, and image inspection. Its fresh reviewer was initially quota-blocked; that blocker is now resolved by the current integrated re-review recorded above. User manual acceptance remains pending. Guide, Practice, footer, persistence, failure/retry semantics, and port `3000` scripts are preserved.

### Historical compact-flow verification — 2026-10-05

| Check | Result |
| --- | --- |
| Root automated gate | PASS: `npm run typecheck` and `../.agents/scripts/run-done-gate.sh .`; lint, 11 test files / 146 tests, Next production build. Final test run started at 00:37:27 UTC+7. |
| Production browser | PASS in isolated Chrome storage: 24 records / eight orders / three producers / four workers, compact non-overlapping cards, six inspector rows, failure reason and producer provenance, retry attempt 2 with unchanged manual commit, reload persistence, Enter/Space/Escape focus, all three source-edge tokens, 24-token cap, expiry, new order keys, producer selection/addition, and Guide send/read/save. Browser error list is empty. |
| Mobile and motion | PASS: `/`, `/customize`, `/guide`, `/practice`, `/practice/hard` at 390px and 320px without page overflow; footer present on every checked route; reduced motion has zero `animateMotion` elements. Mobile opens a readable partition view; pan or fit view explores the complete graph. |
| Root image inspection | PASS: desktop graph, partition inspector, and 390px/320px opening views inspected. The initial desktop graph has 13 nodes and no detailed event rows. |
| Route smoke | PASS: all 11 app routes return HTTP 200 on `http://localhost:3000`. |
| Server ownership | One Next production listener on port `3000`; no listeners on ports `3001`–`3017` at verification. Both `dev` and `start` pin port `3000`; an occupied port fails rather than selecting a new port. |
| Fresh independent review | Initially quota-blocked; superseded by the passing current integrated re-review above. |
| User acceptance | Pending: complete [MANUAL_TEST_CHECKLIST.md](../../docs/MANUAL_TEST_CHECKLIST.md), especially sections 3, 4, 8, and 12. |

Evidence: [browser JSON](artifacts/compact-flow/browser-evidence.json), [route smoke](artifacts/compact-flow/route-smoke.json), [desktop](artifacts/compact-flow/flow-desktop.png), [running](artifacts/compact-flow/flow-running.png), [partition inspector](artifacts/compact-flow/partition-inspector.png), [320px](artifacts/compact-flow/flow-mobile-320.png), and [390px](artifacts/compact-flow/flow-mobile-390.png). The browser checks use temporary storage and do not clear the user's saved workspace. Existing users can explicitly click **Load reference flow** to replace their current simulator/layout with the new reference; hydration preserves their current saved state.

### Repairs completed during integration

- Unread/backlog follows `max(LEO - current read position, 0)`, including after rewind, in visible cards and screen reader labels; latest delivery outcomes remain separate. C means committed next offset and P means current read position throughout the contract.
- Unknown, missing, or conflicting producer provenance cannot pulse a different producer's source edge. Newly produced records keep optional local source provenance even after event history is trimmed; legacy records remain valid with unknown source. Record simulation notes explain that provenance is local, and partition inspection explicitly includes the replication factor.
- Retry focuses a stable inspector control after the retry button disappears. Record inspection preserves the original graph trigger, so Escape returns focus to the partition and Enter/Space can reopen it.

### Exact compact reference workload

- Fresh boot is paused and deterministic: topic `orders`, P0–P3, RF3, `min.insync.replicas=2`, `acks=all`, group `analytics-group`, and four consumers with P0 → `consumer-0`, P1 → `consumer-1`, P2 → `consumer-2`, P3 → `consumer-3`.
- Real producers are `checkout-api`, `payment-service`, and `shipping-service`; each owns eight seeded records and keeps an independent persisted count. Automatic ticks round-robin the existing producer identities; no implicit `producer-1` is created.
- There are eight keys and 24 records: P0 `order-7F3K9`/`order-N8R4C`, P1 `order-Q2M81`/`order-A4D92`, P2 `order-R8H27`/`order-B6T90`, P3 `order-V5X14`/`order-C9L36`; each key has created, paid, and shipped records at offsets 0/1, 2/3, and 4/5 per partition.
- Selected-group seed outcomes are P0/0 committed, P0/1 successful but uncommitted, P1/0 failed attempt 1 with committed offset 0, P2/0 committed, P3/0 read/pending, and all remaining records queued. Total committed records are 2 and initial lag is 22.

### Compact card contract

- The default graph uses approximately 280 × 140 partition cards and an approximately 1330 × 636 desktop canvas projection. Each card shows order count, record count, unread/backlog, selected-group success/failure, and C/P/LEO. `unread = max(LEO - P, 0)`, `backlog = max(LEO - P, 0)`. C means committed next offset, P means current read position, and LEO means log-end offset.
- The default graph has no detailed event table/chart and no payload, timestamp, error, full row, or replica columns. Selecting a partition opens the bounded six-row detail with those fields; `/customize` Activity retains the full event stream. The oldest unresolved failure remains pinned in the detail window and can be retried without changing message identity.
- Producer nodes, counts, source events, message metadata, and transit tokens use the actual producer identity for newly produced records. `/customize` can select a producer; automatic ticks round-robin existing producers. `producerId` is optional local teaching provenance, not an intrinsic Kafka record or wire field; older persisted records remain unknown without migration or reset. Four consumers and their assignments are actual group state, not decorative nodes.
- The opening **Send order** toolbar creates a fresh unique order key. The Guide's dedicated **Send example** action retains `order-7F3K9` for its deterministic lesson.
- Do not automatically migrate or clear saved user positions/viewport. Explicit `Load reference flow` or `Reset workflow` keeps its existing reset behavior; fit view reframes an existing layout.

### Bounded implementation brief for developer and visualization agents

| Owner | Files/surface | Required result |
| --- | --- | --- |
| Domain/state | `src/core/kafka/types.ts`, `producer.ts`, `simulation.ts`, `reference-flow.ts`, `tick.ts`, `src/store/kafka-store.ts`, tests | Add/propagate optional local teaching `producerId` provenance on newly produced records/events; preserve older records with unknown source without migration/reset; seed the exact 3-producer/4-partition/4-consumer/24-record state; round-robin existing producers; persist producer identity/counters; preserve failure, retry, commit, retention, and lag semantics. |
| Visualization | `src/components/KafkaProcessCanvas.tsx`, partition node/card and inspector projections, `src/components/flow-transit.ts`, canvas CSS | Replace default detailed chart/events with compact aggregate cards; keep full six-row payload/timestamp/error/replica detail in partition inspection; render actual producer nodes and matching source edges; keep bounded tokens and keyboard/reduced-motion behavior. |
| Root/customize | `src/components/KafkaVisualLab.tsx`, `ProducerPanel.tsx`, customize surfaces, shared CSS | Add producer selection and short aggregate labels/tooltips; retain explicit reference-load/reset controls, old user layout state, Guide/Practice/footer/storage routes, and port `3000` scripts. |
| Tests/review | domain/store/component tests and browser evidence | Test counts, producer identity/routing, 4-way assignment, compact card metrics, inspector-only details, seed outcomes, persistence, retry identity, no default event table, mobile layout, and no regressions to existing gates. |

### New slice gate

- [x] Default `/` is compact and readable: four aggregate cards, three actual producer nodes, four actual consumer nodes, no detailed event chart/table.
- [x] Inspector/detail reveals the complete bounded rows and technical metadata; Activity retains detailed events.
- [x] Exact seed, producer routing, optional local provenance/unknown legacy behavior, fresh toolbar order keys, C/P/LEO meanings, selected-group outcomes, total committed 2, and initial lag 22 match this handoff and `docs/PROJECT_SPEC.md`.
- [x] Automated gate, production browser, root integration review, and root image inspection pass.
- [x] Current integrated fresh re-review resolves the earlier quota-blocked review gate; current browser readiness is recorded above.
- [ ] Record user acceptance before `COMPLETE`.

## Prior implementation handoff: technical opening chart

- Fresh boot is paused and deterministic: `orders`, P0/P1/P2, three educational brokers, RF3, `min.insync.replicas=2`, `acks=all`, group `analytics-group`, and two consumers.
- Assignment is P0 → `consumer-0`, P1 → `consumer-1`, P2 → `consumer-0`.
- Rows are P0/0 `order-7F3K9` committed; P0/1 `order-N8R4C` processed but uncommitted; P1/0 `order-Q2M81` failed with a reason; P1/1 `order-A4D92` queued; P2/0 `order-R8H27` queued.
- Reset returns the empty three-partition learning state. `Load reference flow` is explicit and must not overwrite persisted user state during hydration. The Guide one-record exercise remains unchanged.

### Historical verification snapshot — prior technical chart

- Latest root gate: `npm run typecheck` PASS; lint PASS; 11 test files / 134 tests PASS; Next production build PASS.
- Fresh final reviewer and UI image review: 0 unresolved P0/P1 findings. The previously reported public transaction admission finding is fixed and regression-tested; the retained logical-history growth is a documented P2 limitation.
- Production-browser evidence: reference rows, failed record inspector, retry with attempt 2 and unchanged manual commit, reload without token replay, reset/reference loading, Guide's separate one-record exercise, footer on five routes, 390/320px layouts without overflow, reduced motion with zero `animateMotion`, six-row pinned-failure and scroll behavior, no partition overlap, pulse expiry, 24-token cap, and empty console/error lists all pass on the latest production run. All 11 routes returned HTTP 200.
- Final evidence is recorded in [browser-evidence.json](artifacts/technical-chart/browser-evidence.json), [flow-six-rows.png](artifacts/technical-chart/flow-six-rows.png), [flow-six-rows-scrolled.png](artifacts/technical-chart/flow-six-rows-scrolled.png), [flow-mobile-320.png](artifacts/technical-chart/flow-mobile-320.png), [flow-mobile-390.png](artifacts/technical-chart/flow-mobile-390.png), and [failed-inspector.png](artifacts/technical-chart/failed-inspector.png). That historical milestone reached `READY_FOR_MANUAL_TEST`; current packet/learning readiness is recorded above, and user acceptance remains unchecked.

### Bounded implementation brief

| Owner | Files/surface | Required result |
| --- | --- | --- |
| Root/orchestrator | `src/app/layout.tsx`, `src/components/LabAppShell.tsx`, `src/components/KafkaVisualLab.tsx`, `src/components/AppFooter.tsx`, `src/config/site-profile.ts`, global shell CSS | Shared black/warm-white/gray/red footer with copyright. The public profile is intentionally empty until supplied, so it shows `Creator details pending`; no identity is guessed. Preserve explicit reference-load control and route contract. |
| Domain/state developer | `src/core/kafka/types.ts`, `consumer.ts`, `offsets.ts`, `cluster.ts`, `simulation.ts`, `src/store/kafka-store.ts`, hooks, tests | Add group/attempt-scoped application success/failure, failure event/action/retry, exact seed, `minInSyncReplicas`, and `acks=all` current-ISR/min-ISR semantics. Never remove topic records after read/processing/commit. |
| Visualization | `src/components/KafkaProcessCanvas.tsx`, partition chart/projection components, `flow-transit.ts`, canvas CSS | Show bounded ordered partition rows and technical headers; expose queued/read/processed/failed/committed/skipped with attempt/error; keep graph first, no per-record nodes, commit token as checkpoint path. |
| Content/QA | `docs/KAFKA_KNOWLEDGE_AUDIT.md`, Guide, Advanced, inspectors, scenarios, quiz sources | Audit every learner-facing claim against current Apache sources; qualify broker/protocol simplifications, commit semantics, acks/min ISR, liveness, transactions, and durability. |

### Invariants

1. `KafkaMessage` remains the stored partition record. Application outcome is only on `DeliveryAttempt` and is scoped by group, consumer, and attempt.
2. The failure action does not advance committed offsets or remove the record. Both commit modes stop before the latest failed attempt and emit `offset-commit-blocked`; current-position/commit-too-soon skips only pending unprocessed work in this lab. Real Kafka permits explicit commits past failed work; retry keeps message ID/partition/offset and increments attempt.
3. `record-processed` means application success for that attempt; it does not imply commit. `offset-committed` means only a group checkpoint. The visual token ends at a checkpoint path and does not imply a payload was sent to an offset node.
4. `acks=all` is accepted when current ISR meets `min.insync.replicas` under the educational cluster policy; it does not require every configured replica or consumer.
5. Visible queues are bounded to a six-row window and sorted by partition-local offset. The oldest unresolved failure is pinned, visible rows remain scrollable, and rows outside the window are summarized. No record becomes a React Flow node; topic rows remain after reads, failures, and commits. Latest group/message outcomes remain available while superseded attempts use the 200-attempt history budget.
6. The default reference state is honest local teaching data. It must be labelled paused/educational and must not be presented as live broker traffic or complete Kafka wire behavior.
7. Activity pulses use an approximately two-second `1800ms` visual TTL and clean up without changing simulation state. The execution strip separately labels `stored`, `checkpoint Σ`, and `commit lag`.
8. Keyboard `Enter` on a record row opens only the record inspector; it must not also select the parent partition.

### Verification gate for this slice

- [x] Add focused engine/store tests for the exact seed, outcomes, retry identity, retention, commit boundaries, and ISR/min-ISR acks.
- [x] Add projection/UI tests for sorted rows, metadata, selected-group state, error reason, bounded rows, footer routes, commit blocking, and activity cleanup.
- [x] Complete the claim table in `docs/KAFKA_KNOWLEDGE_AUDIT.md`.
- [x] Run typecheck and done gate: latest typecheck, lint, 11 test files / 134 tests, production build, and browser rerun all PASS.
- [x] Fresh narrow keyboard/focus reviewer and UI image review confirm 0 unresolved P0/P1 findings; the six-row and mobile images are readable and pass their layout checks.
- [x] Return status to `READY_FOR_MANUAL_TEST` after the final browser and image evidence; user acceptance is still required before `COMPLETE`.

## Implemented surfaces

| Surface | Expected behavior | Main files |
| --- | --- | --- |
| Flow `/` | Live nodes/links, compact run/pause/sample/reset, selected inspector, status strip | `src/components/KafkaVisualLab.tsx`, `src/components/KafkaProcessCanvas.tsx`, `src/components/process-canvas.css` |
| Shared navigation/theme | Flow, Customize, Guide; warm light surfaces; keyboard and mobile layouts | `src/components/LabAppShell.tsx`, `src/components/LabIcon.tsx`, `src/components/workspace.css`, `src/app/layout.tsx` |
| Customize `/customize` | Workflow, Messages, Workers, Examples, Advanced, Activity | `src/components/CustomizeWorkspace.tsx`, existing detail panels, `src/app/customize/page.tsx` |
| Guide `/guide` | Original one-record lesson plus five collapsed prediction/run/observe/explain experiments | `src/components/GuidePage.tsx`, `GuideExperiments.tsx`, `src/learning/experiments.ts`, `src/app/guide/page.tsx` |
| Learning progress | Independent validated prediction/completion under `kafkalab:learning:v1`; no restored live results | `src/learning/progress.ts`, `tests/components/guide-experiments.test.tsx` |
| Runtime state | One Zustand instance during navigation; topology/reset clears state; reading respects configured rates | `src/store/kafka-store.ts`, `src/hooks/useKafkaLab.tsx` |
| Record transit | Event-aged actual SVG edge painting, stable generations, finite original deadlines, exact endpoint frame, and bounded cleanup; main browser displacement passes | `src/hooks/useFlowActivity.ts`, `src/components/FlowPacket.tsx`, `flow-transit.ts`, `KafkaProcessCanvas.tsx`, focused component tests |
| Delivery lessons and generated IDs | Explicit Commit too soon result; processed commits never falsely report loss; unique consumers after scenario setup | `src/scenarios/index.ts`, `src/core/kafka/offsets.ts`, `src/store/kafka-store.ts`, `src/components/ScenarioPanel.tsx` |
| Regression coverage | Pure semantics, state/rates/reset, Guide and customization interactions | `tests/core/kafka.test.ts`, `tests/store/kafka-store.test.ts`, `tests/components/route-workspaces.test.tsx` |

## Historical evidence — original UI redesign

| Required check | Result |
| --- | --- |
| Affected specification, stories, criteria, and continuation checklists reconciled | Updated for compact Flow actions/selected inspection and Guide exercise |
| Type checking | PASS: explicit `npm run typecheck`; final production build also passed embedded TypeScript checking |
| Done gate | Historical redesign run: `.agents/scripts/run-done-gate.sh`, 2026-10-03 22:22:33 UTC+7; lint PASS, 3 test files / 56 tests PASS, production build PASS |
| Production build routes | `/`, `/customize`, `/guide`, and `/icon.svg` generated successfully |
| Local production server | Historical redesign run: `npm run start -- --port 3000`; current route smoke is recorded below |
| Production browser checks | PASS in isolated headless Chrome; console, page, and network errors were empty |
| Canvas activity | 7 visible nodes / 8 edges; running produced actual event packets; paused packets expired to 0. Earlier manual sample produced 2 packets, then expired to 0 |
| Topology and layout | PASS: 8 partitions → 12 nodes / 18 edges; second worker → 13 nodes / 19 edges; drag changes layout only and reset restores it |
| Guide and custom sending | PASS: one exact sample → read existing only → manual save; Flow shows 1 sent / 1 saved / 0 awaiting commit. Custom send and valid sending after partition shrink passed |
| Responsive/keyboard/motion | PASS: all three routes at 390px and 320px without page overflow; repeat Enter → Escape → Enter edge inspection returns focus to SVG edge; reduced motion has 0 animateMotion elements |
| Commit-before-processing loss demonstration | PASS: Commit too soon inspector reports stored 1 / saved next offset 1 / processed 0 / restarted reads 0 in the production browser |
| Scenario consumer additions | PASS in development browser: six additions after Consumer scaling, Rebalance, and Slow consumer reached 11, 8, and 7 workers respectively without errors |
| Example guidance and inspector context | How to try it exposes Goal/Action/Look for/Why; commit inspector includes selected group and per-partition offsets |
| App icon | Added `src/app/icon.svg`; root reports favicon 404 resolved |
| Fresh reviewer, zero P0/P1 | Historical UI redesign review; current review is recorded below |
| User manual acceptance | Not performed; follow `docs/MANUAL_TEST_CHECKLIST.md` |

## Previous milestone: Practice, interview guide, and persistence

| Required check | Result |
| --- | --- |
| Type checking | PASS: `npm run typecheck` |
| Done gate | PASS: `../.agents/scripts/run-done-gate.sh .`; lint PASS, 6 test files / 74 tests PASS, production build PASS (historical milestone evidence) |
| Production route smoke | PASS: HTTP 200 for `/`, `/customize`, `/guide`, `/practice`, and `/practice/{easy,middle,hard,extreme-hard,most-common,trick,situation}` |
| Fresh integrated review | PASS: 0 unresolved P0/P1; one P2 remains for logical message/delivery history growth during continuous production, documented under Runtime limits |
| User manual acceptance | Pending; follow `docs/MANUAL_TEST_CHECKLIST.md`, especially sections 10 and 11 |

## Historical milestone: Individual record transit

| Required check | Result |
| --- | --- |
| Event identity | PASS: produced event `entityId` survives the store adapter; poll events include `messageId`; commit events expose exact partition/offset ranges |
| Transit projection | PASS: produced, routed, polled, and committed phases use matching stable edge IDs, message IDs, attempts, and source event IDs |
| Bounds and persistence | PASS: 24-token visual cap; tokens are not React Flow nodes or persisted workspace state |
| Reduced motion | PASS by code/tests: no motion token animation when reduced motion or page visibility disables motion; browser confirmation remains manual |
| Automated checks | PASS: `npm run typecheck`; done gate with lint, 7 test files / 87 tests, and production build |
| Production route smoke | PASS: HTTP 200 for all 11 routes on `http://localhost:3017` |
| QA comparison | PASS: malformed produce/poll events without a message ID or preserved entity ID are dropped; mismatched identities are rejected |
| Fresh independent reviewer | PASS: 0 unresolved P0/P1; one known P2 remains for retained logical history growth during long sessions |
| User manual acceptance | Pending: run Flow token checks in `docs/MANUAL_TEST_CHECKLIST.md` sections 3, 8, and 11 |

## Historical visual evidence — original UI redesign

Saved production-browser screenshots, refreshed after the final build: [Flow](artifacts/ui-redesign/flow-desktop.png), [running flow](artifacts/ui-redesign/flow-running.png), [Customize](artifacts/ui-redesign/customize-desktop.png), [Guide](artifacts/ui-redesign/guide-desktop.png), [mobile Flow](artifacts/ui-redesign/flow-mobile.png), [mobile Customize](artifacts/ui-redesign/customize-mobile.png), and [mobile Guide](artifacts/ui-redesign/guide-mobile.png). These show the checked rendering and do not replace the user acceptance checklist. The root/orchestrator supplied the command and browser evidence above; improvement-qa.bot reconciled the documentation and recorded it.

## Resume and repair

1. Use the final packet/learning evidence at the top of this handoff and [PACKET_LEARNING_REVIEW.md](PACKET_LEARNING_REVIEW.md): all code/automated/review/browser/image gates pass. Open the existing production app at `http://localhost:3000` and follow section 14 of `../docs/MANUAL_TEST_CHECKLIST.md`, then relevant earlier regression items. Natural tab-backgrounding and user acceptance remain open; automated background coverage used an isolated visibility-state override.
2. Use the file map to locate the affected layer. Keep the simulation engine independent of React and Zustand.
3. Before switching server modes or rebuilding, run `npm run stop` from nested `kafkalab/` and verify port 3000 is free with `lsof -nP -iTCP:3000 -sTCP:LISTEN` (no listener output). Then run `npm run typecheck` and `../.agents/scripts/run-done-gate.sh .` after a repair and record exact results. Passing automated checks do not prove browser behavior.
4. Start one chosen server (`npm run dev`, or `npm run start` after the build), then repeat the browser checklist. Request fresh review after fixes; route P0/P1 findings back to implementation.
5. Keep user acceptance unchecked until the user completes it. Use `COMPLETE` only after the required manual checklist passes; a later failing change returns the task to `IN_PROGRESS`.

## Runtime limits

- The app simulates Kafka in one browser tab. It does not connect to a real broker or implement Kafka's network/distributed protocols.
- Older redesign evidence predates Practice and persistence. Current behavior persists versioned simulator, chart/timeline, flow layout, quiz progress, and independent `kafkalab:learning:v1` progress in browser `localStorage`; reload restores playback paused without restored packets or captured lesson results. Applying topology, loading examples, and simulator reset replace simulator state while Practice and learning progress are kept. See `docs/MANUAL_TEST_CHECKLIST.md` section 14 for the current slice and sections 10/11 for earlier persistence checks.
- Advanced delivery policies that are represented only as state remain explicitly labelled educational.
- Current runtime boundary: stored topic messages and latest group/message delivery outcomes can grow during continuous production, while superseded delivery attempts use the `maxDeliveries=200` budget. Graph/render windows, six-row partition windows, event/chart history, and packet activity remain bounded; the oldest unresolved failure is pinned and visible rows remain scrollable. Use short experiments with pause/reset because browser quota and total runtime memory remain finite.
- No zip, commit, push, or deployment is part of this handoff.
