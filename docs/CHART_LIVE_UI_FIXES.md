# Live connections across all seven charts

Status: `READY_FOR_MANUAL_TEST` — 2026-10-07. Implementation, typecheck/lint, 193 tests across 18 files, production build, and refreshed browser checks pass. Final independent source re-review reports P0/P1/P2 = 0 within this repair scope. User acceptance remains open. See [repair evidence](../.agents/review/CHART_LIVE_UI_REVIEW.md) for exact checks and limits.

## Scope and instruction precedence

Repair Flow, Partitions, Consumers, Offsets & lag, Delivery, Replication, and Activity using the existing client-side simulator and graph renderer. Each view must show finite activity on the relationships affected by a real event or an identifiable state transition. Running playback alone is not evidence that every connection is busy.

The current user explicitly requested fixes and checks. Normal meaningful regression verification is appropriate, including focused tests and the lint/test/build gate. The no-test language in the historical theme/dialog slice, agent instructions, orchestration skill, and review documents applies to that earlier slice only. Its recorded `NOT RUN` result remains historical evidence and must not be rewritten as a pass for this work.

The current route contract in [PROJECT_SPEC.md](PROJECT_SPEC.md), app `AGENTS.md`, and [manual section 16](MANUAL_TEST_CHECKLIST.md#16-focused-chart-tabs-and-tooltips--current-slice) remains authoritative: `/` has Run/Pause, Reset workflow, seven tabs, and contextual tooltips; `/customize` returns 404. Older sample/reference buttons, configuration panels, execution strips, StepInspector, and replacement dialogs are not restored to satisfy old stories/tests. Guide, Practice, learning progress, theme, and footer behavior remain required regressions.

QA ownership for this handoff is only this file, [IMPLEMENTATION_CHECKLIST.md](IMPLEMENTATION_CHECKLIST.md), and [CHECKLIST.md](../CHECKLIST.md). Root is investigating and owns renderer implementation. No app source, server, process, dependency, or deployment changes are part of this QA handoff.

## Affected requirements and observed gaps

Affected stories are US-03–08 (consumer progress, groups, lag, rebalance, delivery), US-09/11/12 (explanation, accessibility, replication), US-17–19/25 (relationships, layout, lively opening), US-29–33/37/39 (persistence, identity, outcomes, compact state), and US-42–44 (finite lifecycle, dialog removal, theme). Relevant specification sections are event identity, route experience, process-flow canvas, delivery detail, and persistence/performance. Applicable acceptance criteria are process-flow/accessibility, record transit, compact partition/outcome semantics, and local persistence. Roadmap milestones 4, 7, and 8 plus process-flow Track 4 and reliable-transit Track 10 supply the existing scope; this is a repair slice, not a new infrastructure track.

Initial source inspection at the documentation handoff found these concrete review targets. They are now repaired; runtime evidence is linked above:

- `KafkaProcessCanvas.tsx` gates motion and transit projection to `variant === "flow"`; non-Flow edges have no independent causal activity mapping.
- `useFlowActivity.ts` accepts only `message-produced`, `consumer-polled`, and `offset-committed`, leaving processing/failure, rebalance, and broker transitions without pulses.
- `chart-views.ts` emits Flow commit edge IDs as `consumer-${consumerId}-commit`, while `flow-transit.ts` selects `consumer-commit-${consumerId}`. Reconcile the selector and rendered edge, including existing regression expectations.
- Tooltip position currently tracks window resize/scroll but not graph transforms; the node frame has no explicit Escape dismissal. Verify positioning, hover/focus lifetime, clipping, and mobile usability while repairing these surfaces.

## Shared activity contract

An activity descriptor is presentation data derived from the existing snapshot/events, not another Kafka model. It identifies the chart variant, visual generation, source event ID, exact stable relationship/edge ID, phase, and affected entity. Record activity additionally retains message ID, topic/partition/offset, attempt, and applicable producer/group/consumer IDs. State-only activity instead identifies the actual changed field/entity and before/after values; it must not fabricate a record or source event ID. A topology or assignment transition identifies the affected group, partition, owner, broker, or replica relationship.

Use structured metadata and actual snapshot identity together. Do not route by summary text, the latest arbitrary message, group-wide counts, or numeric ID coincidence. Missing or conflicting metadata suppresses only the decorative activity; the event, counters, and `unknown`/`unavailable` explanation remain. Group-specific activity must match the selected group. A disappeared or reassigned relationship cannot receive an old token.

At most 24 transient markers/descriptors are rendered across the visible chart, using the newest valid activity. The event-deduplication window is bounded to the newest 200 events, consistent with the current activity hook; graph cardinality follows the bounded projection, never one node per message. Each accepted activity has one finite start/deadline within the existing `1800ms` pulse window. Delays, multiple phases, rerenders, path changes, and overflow must not restart that deadline. Moving packets paint a destination frame before removal; any endpoint cleanup fallback is bounded to `64ms`. Old retained/seeded events do not become new activity on mount.

Persistent attention (lag, failed work, broker down) and accumulated counts are state, not an endless pulse. When no qualifying transition occurs, connections remain readable and still. Animation frames, markers, timers, deduplication ledgers, and visual generations are excluded from localStorage and never advance offsets, produce/consume records, or change outcomes.

## Activity required for each chart

| Chart | Causal connection activity and identity | Truth boundary |
| --- | --- | --- |
| Flow | A newly appended message uses its actual producer → topic → allocated partition path. Poll uses its event group, assigned consumer, partition/offset, message, and attempt. Commit uses the actual consumer → checkpoint edge and only message IDs or offsets in `[previous, committedOffset)`. | Unknown producer provenance stays unknown. A checkpoint is not payload deletion or proof of business success. Selector/rendered edge IDs must match. |
| Partitions | Append pulses only topic → the event's partition. Poll pulses that partition → its actual selected-group owner. Assignment/rebalance changes identify the affected partition/owner relationship; removed ownership links stop carrying activity. | Other partition lanes remain still. Counts, unread/backlog, C/P/LEO, and latest outcomes stay snapshot-derived. Commit may update the partition state without inventing a commit edge. |
| Consumers | Poll/process/failure/commit pulses identify the real selected-group consumer and partition relationship. Join/leave and assignment events affect only their member/ownership links. | An `owns` arrow depicts assignment; its pulse is not a record travelling backward from consumer to partition. Idle/unassigned consumers do not receive fabricated traffic. Rebalance does not imply every consumer polled. |
| Offsets & lag | Append updates the exact partition → LEO connection; poll updates partition → P; a real commit advances partition → C. Capture event identity and actual before/after values, including rewind/restart where supported. | These are metric-change indicators, not payload packets. Process/failure alone does not advance C; blocked/no-op commits show their reason without a false C advance. Lag remains `max(LEO - C, 0)` and unread remains `max(LEO - P, 0)`. |
| Delivery | Project real selected-group message/attempt transitions: queued → in-flight on poll, application success or failure on its result event, failed → in-flight on a new retry attempt, and a checkpoint/skip only for an actual committed range. Add/correct view-only relationships where the current stage graph omits a truthful failure/retry path. | Preserve the same message ID on retry with a new attempt. Do not force a successful delivery through an invented timed sequence, double-count overlapping processed/succeeded summaries as separate records, or depict a blocked commit as success. Failure retains the stored record and C. |
| Replication | Append pulses topic → its partition and finite `ISR placement` relationship signals for that partition's current live ISR model. Broker failure/recovery and leader/ISR changes pulse only affected partition ↔ broker relationships using actual broker IDs, assignments, previous/current leader or ISR membership, and source event/state transition. | The current cluster model has placement/health metadata, not per-replica record-transfer progress. Broker signals carry no record identity and are labeled as modeled placement; they do not assert replica packet transfers, synchronization, or acknowledgements. An absent cluster stays explicitly unavailable with no fake traffic. Ordinary playback need not change every broker link. |
| Activity | Each new event pulses activity-root → its actual bounded category: produced, read, processed, failed, committed/blocked, rebalance, or broker. Preserve the source event ID/type and available entity/group/partition/attempt identity; counts and latest detail come from retained events. | Historical category counts do not keep a line moving. `offset-commit-blocked` identifies blocked work distinctly from an advanced checkpoint. Events from another group may appear as labelled history but cannot impersonate selected-group record transit. |

## Lifecycle and geometry criteria

- Pause cancels in-flight decorative motion, pulses, animation frames, and delayed callbacks; counters/state remain inspectable. Resume accepts new transitions without replaying pre-pause activity. An explicit Guide action while playback is paused may create a fresh finite indicator for its own real event; paused playback alone creates none.
- Reset/workflow replacement increments the visual generation, invalidates pending phases and deduplication boundaries, clears chart activity/layout according to the existing reset contract, and cannot allow old callbacks to revive a marker. Practice, learning, and theme reset independence remains intact.
- Switching chart tabs clears the outgoing transient layer and tooltips, establishes the incoming current-event watermark, and renders its present snapshot/layout. Switching back cannot replay retained events or remount an old packet with a fresh clock. Switching tabs does not pause or reset the shared simulation.
- Route unmount, hydration/reload, page hiding, or selected-group change invalidates stale visuals and closes orphan tooltips. Reload restores serializable data/layout with playback paused; returning to a route/visible tab waits for a new relevant event. Hidden views retain no growing animation queue.
- Reduced motion suppresses travelling markers, animated dashes, pulsing/glowing loops, and `animateMotion`; matching connection/state text, current metrics, and tooltips stay readable. Changing the preference during transit cancels movement immediately and cannot replay the old transit when switched back.
- Every edge uses the current measured source/target handles and positions. During and after dragging either endpoint, the visible connection, arrowhead, label, and any activity marker use the same recomputed path. This also holds after pan/zoom, resize, node-size changes, topology changes, tab restoration, and saved-layout reload. Updating geometry must not restart event time or alter Kafka state.
- Preserve stable relationship IDs, per-variant positions/viewports, selection, and keyboard access. Removed nodes/edges leave no orphan marker, selection, tooltip, duplicate SVG ID, or React key/handle warning. Keep visual/layout updates scoped to the active chart.

## Accessible and responsive tooltips

Show the same contextual details on pointer hover and keyboard focus with `role="tooltip"`, a stable unique ID, and `aria-describedby` on the focused trigger. Keep a visible focus indicator and meaningful node/edge/tab names. Escape dismisses without moving focus; it must remain dismissed until a new intentional hover/focus interaction. A tooltip persists while its trigger or tooltip is hovered, closes when both hover and focus leave, and is cleared on tab/reset/unmount or entity removal. It cannot become a click-pinned inspector, dialog, or focus trap.

Anchor tooltip geometry to the current trigger in viewport coordinates even when React Flow pans, zooms, drags, or resizes. Keep it outside transformed/clipped graph layers or use equivalent correct coordinate conversion; clamp using actual content dimensions and available viewport space. At desktop and 320px/390px widths in both themes, text wraps, long IDs/values remain readable, and no tooltip causes page-level overflow or blocks navigation/playback controls. Prefer bounded summaries that fit; any necessary local scrolling remains usable with pointer/touch/keyboard without trapping focus. Touch users can focus an existing trigger to read the same details and dismiss normally.

Counts, offsets, failure reasons, ownership, broker/ISR metadata, and latest event details update with the snapshot while open. Missing values say `unknown` or `unavailable`; color alone never conveys phase, failure, or selection. Avoid live-region announcements on every animation frame. No console/runtime/hydration, invalid DOM, duplicate ID/key, or missing-handle errors may be introduced.

## Bounded implementation brief and verification

Root/developer owns `kafkalab/src/components/KafkaProcessCanvas.tsx`, `chart-views.ts`, `flow-transit.ts`, `FlowPacket.tsx`, `process-canvas.css`, and `kafkalab/src/hooks/useFlowActivity.ts` for this repair, with existing focused component/store tests as needed. Coordinate `KafkaVisualLab.tsx`, shared styles, hooks/store adapters, or a small pure projection helper only when the repair requires them. Keep the core engine, route removals, persistence schemas, server configuration, dependencies, and unrelated product work outside the change unless a concrete additional need is reported to root.

Implement event-to-relationship mapping first, then finite lifecycle/rendering for each variant, then shared geometry and tooltip repairs. Resolve ID/direction mismatches against the actual projected graph. Include focused meaningful regressions for exact routing, wrong-group/unknown metadata, retry and commit ranges, changed offset fields, broker transitions/no-cluster state, lifecycle invalidation, the 24 cap, drag geometry, and tooltip behavior. Static labels or a CSS animation class alone do not prove live activity.

At the original documentation-only handoff, verification was **PLANNED / NOT RUN**. Root subsequently completed the checks below; current PASS results and honest browser limits are recorded in [CHART_LIVE_UI_REVIEW.md](../.agents/review/CHART_LIVE_UI_REVIEW.md). From the nested `kafkalab/` app directory:

```bash
npm run typecheck
npm run lint
npm run test
npm run build
```

During development, focus on the existing `chart-views`, `chart-tabs`, `chart-tooltips`, `flow-projection`, `flow-transit`, `flow-packet`, `flow-activity`, `route-workspaces`, and local-persistence tests; add only coverage justified by the changed behavior. The bundled done gate may supply final lint/test/build results once implementation is stable. This QA handoff does not execute it or start/stop/rebuild a server. A fresh independent review must report zero P0/P1 findings before `READY_FOR_MANUAL_TEST`; prior test totals/screenshots do not certify this slice.

Before requesting user acceptance, root should verify the following in the existing browser session without this QA bot changing server state:

1. Trigger actual append, poll, success/failure, retry, and commit through existing Run/Guide actions or deterministic test fixtures. Across all seven tabs, record the source event/state identity and exact affected connection; unrelated connections stay still and indicators expire. Use broker failure/recovery and a missing-cluster fixture for Replication, rather than expecting ordinary Run to create broker transitions.
2. Observe displacement of the same identified packet/indicator over time, its destination/expiry, and the bounded count. Pause/resume, reset, switch every tab, change group, navigate, reload, hide/show, and toggle reduced motion during transit; confirm no stale replay/callback growth and correct preserved data.
3. Drag both connection endpoints during activity on each tab, then pan/zoom/resize and restore saved layouts. Verify line/arrow/label/marker alignment and independent positions/viewports with no state mutation.
4. Exercise hover/focus/Escape/touch tooltip lifetime and open-tooltip snapshot changes in both themes at desktop, 320px, and 390px; verify readable labels, tab keyboard navigation, no overflow/clipping/focus trap, and no console/runtime errors.
5. Run still-applicable manual section 16 and Guide/Practice/theme/footer/persistence regressions. Record automated/browser PASS or FAIL separately from pending user acceptance. Remain `IN_PROGRESS` while required implementation/checks remain; use `COMPLETE` only after manual acceptance.
