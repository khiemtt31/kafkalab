# Packet lifecycle and learning review

Date: 2026-10-05. Status: `READY_FOR_MANUAL_TEST`. User acceptance is open.

## Assessment and implementation

The earlier Guide supported introduction and interview recall, but did not require a causal prediction or a comparison with actual state. Five collapsed experiments now require prediction, explicit Run, captured Before/After evidence, explanation, interview follow-up, and a stated simulation boundary. They cover keyed ordering, replay before commit, commit before processing, worker scaling, and failure/retry/manual commit. Observations come from the shared store/core actions. Completion records participation, not production mastery.

Learning predictions/completion use independent validated `kafkalab:learning:v1` storage. Restoring them never runs an experiment or restores old evidence as live state. The original Guide send/read/save exercise remains available.

The bounded workstreams were root implementation/orchestration, `/root/learning_contract` for docs-first improvement QA, `/root/packet_review` for read-only packet diagnosis, and `/root/fresh_learning_review` for independent source review. Root alone ran shared build/server/browser checks.

The original stuck packet used SVG document timing and retained its endpoint. A native animation start alone also failed the browser displacement assertion. The final renderer paints the actual edge geometry using `requestAnimationFrame` and `getPointAtLength`, with an original event birth/deadline, stable generation keys, delayed routing, and finite cleanup. It paints the exact destination for a frame, with a 64ms cleanup fallback; all current segments finish below the 1800ms presentation TTL. Pause, navigation/reload, fixture replacement, and visibility changes invalidate stale activity without changing real records or offsets. Reduced motion keeps finite static indicators.

## Fresh independent source review

Reviewer: `/root/fresh_learning_review`, read-only and separate from implementation. Final review found **P0 = 0, P1 = 0, actionable P2 = 0**.

Resolved findings:

- Show the complete assignment map and idle member IDs.
- Show the captured failure reason.
- Guard retry commit against changed ownership, attempts, restart/read position, group, or workflow; report completion only after the actual checkpoint advances.
- Label inspection as **Inspect current Flow**, since earlier captured evidence can differ from the current workflow.
- Paint the exact packet destination before bounded removal.

The final scroll-region refinement was reviewed too. A scoped `min-width: 0` allows the disclosure to shrink while the 540px comparison table scrolls locally. Labels, native table headers/captions, keyboard focus, a focus outline, and the mobile swipe hint remain available.

## Root verification

| Check | Evidence |
| --- | --- |
| Type checking | `npm run typecheck` PASS after the final code changes. |
| Required gate | [done-gate.log](artifacts/packet-learning/done-gate.log): lint, 14 files / 167 tests, and production build PASS. No npm `test:e2e` script is configured; separate browser checks are recorded below. |
| Actual movement and five experiments | [after-fix.json](artifacts/packet-learning/after-fix.json): three late sends change their actual CTM on the same packet; routed segments move with matching identity; expiry, route/reload/pause/reset, reduced motion, 24-token cap, original Guide, all five actual lesson outcomes, saved progress without execution/result replay, 11 HTTP-200 routes, and zero browser errors PASS. |
| Existing app regressions | [compact regression](artifacts/packet-learning/compact-regression/browser-evidence.json): compact geometry, inspector details/retry, rewind/unread, Enter/Space/Escape and focus return, all producer sources observed across finite transits, producer selection/addition persistence, original Guide, five routes at 320px/390px, reduced motion, footer, and zero browser errors PASS. |
| Final mobile/storage refinement | [supplemental-browser.json](artifacts/packet-learning/supplemental-browser.json): native ArrowRight scrolls the focused table by 40px at both mobile widths; local scroll ranges are 232px/302px; document width stays 390px/320px. Real answered-quiz, simulator, and learning resets preserve each other's data. Invalid learning entries are ignored and blocked storage keeps experiments usable in memory. Zero browser errors. |
| Root image inspection | Desktop Guide/Flow and final 320px comparison before/after horizontal scrolling inspected. See [mobile comparison](artifacts/packet-learning/guide-evidence-viewport-320.png), [scrolled comparison](artifacts/packet-learning/guide-evidence-scrolled-320.png), and [moving packet](artifacts/packet-learning/packet-moving.png). |
| Final routes/server | [route-server-smoke.json](artifacts/packet-learning/route-server-smoke.json): all 11 app routes return HTTP 200; one Next listener on 3000, none on 3001–3017. |

Browser checks used isolated temporary Chrome profiles, not the user's saved workspace. The background browser trial explicitly overrides `document.visibilityState` and dispatches `visibilitychange`; hook tests cover the handler. Natural tab backgrounding remains a user manual check. Before-fix and native diagnostics remain in [before-fix.json](artifacts/packet-learning/before-fix.json) and [packet-debug.json](artifacts/packet-learning/packet-debug.json); they are not final acceptance results.

Recorded browser scripts are beside their evidence: [main browser QA](artifacts/packet-learning/browser-qa.mjs), [mobile/storage QA](artifacts/packet-learning/supplemental-browser-qa.mjs), and [compact regression](artifacts/packet-learning/compact-regression/browser-qa.mjs). Run them sequentially with Node against the already running port-3000 app. They require Chrome (default macOS path; override with `KAFKA_QA_CHROME`) and use temporary profiles plus debugging port 9228.

## Continue manually

Open `http://localhost:3000`. Follow [manual checklist section 14](../../docs/MANUAL_TEST_CHECKLIST.md#14-packet-lifecycle-and-guided-experiments--current-slice), then relevant earlier regression checks. Record failures and reproduction steps in the handoff. Keep `READY_FOR_MANUAL_TEST` until the user accepts the required checks; then mark `COMPLETE`.

This local browser simulator does not establish production Kafka operational mastery. Real client/broker failures, operational metrics, distributed protocols, and externally verified application side effects remain practical study boundaries.
