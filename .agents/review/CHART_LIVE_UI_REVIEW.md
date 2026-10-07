# Live chart UI repair — 2026-10-07

Status: `READY_FOR_MANUAL_TEST`; implementation, root verification, browser checks, and independent re-review pass. User acceptance is open.

## Changes

All seven charts project finite activity from actual simulator events onto their own stable connections. Flow's checkpoint edge ID now matches the transit selector. Consumer reads travel toward their actual owner, offset append/read/commit indicators target LEO/P/C, and captured crash/restart/assignment changes show actual read-position changes. Crash indicators match the previous owner, restart indicators match the current owner, and conflicting actor metadata suppresses decorative activity. Production highlights the affected partition's live ISR placement model with record-free relationship signals; it does not claim measured replica transfer or acknowledgements.

Delivery has six mutually exclusive states, so each stored record is counted once. One successful processing event produces one direct success transition. Failure, retry, skipped work, and checkpoint activity remain distinct. Signals retain available record/partition/offset/attempt identity, multi-record checkpoint signals are deduplicated, and the 24-token cap applies after choosing the active chart's relationships.

Connections update while whole cards are dragged. Offset metric cards and partition groups no longer overlap. Each chart restores its own layout; mobile restores recenter the chart only when its focal node would be outside the visible canvas. Tab/group changes, reset, pause, hidden pages, and reduced-motion changes invalidate stale presentation activity.

Tooltips render outside the transformed canvas, use measured viewport placement, follow graph geometry, remain available while hovered or focused, and permit pointer/keyboard scrolling. Escape dismisses without a focus trap. ReactFlow wrapper focus exposes the details without a second tab stop, early focus is captured, and hiding the page closes details and cancels their geometry tracking. The Next.js development badge is disabled because a real mobile click on the Flow tab hit that badge instead.

## Root verification

- `npm run typecheck`: PASS.
- `npm run lint`: PASS.
- `npm test -- --run`: PASS, 18 files / 193 tests.
- `npm run build`: PASS, Next.js 16.3.8 production output for Flow, Guide, Practice and all seven quiz categories.
- `node .agents/scripts/check-chart-ui.mjs`: PASS against the running development app on `http://localhost:3000`.

Final browser evidence is [evidence.json](artifacts/chart-live/evidence.json); the repeatable script is [check-chart-ui.mjs](../scripts/check-chart-ui.mjs). It uses an isolated temporary Chrome profile and an automatically allocated debugging port, then closes its own browser and removes that temporary profile. The user's browser storage is untouched.

The browser check verifies the same identified marker changes screen position on every chart, each chart's real connection changes after card dragging during playback, the token count stays at or below 24, and pause removes tokens/active edges/node pulses. It checks all seven tooltip surfaces, Escape, both themes, native tab clicks at 390px/320px, viewport bounds, no page overflow, offset-card separation, finite static reduced-motion indicators, Guide/Practice route smoke, `/customize` HTTP 404, and zero runtime/console errors. Screenshots are stored beside the evidence. Root inspected mobile and chart images. The earlier `failure.png` is diagnostic evidence, not the final passing result.

## Independent review

The first fresh Kafka/UI review found one P1 and six actionable P2 issues: duplicate Delivery counts/transitions, duplicate checkpoint keys, missing rewind activity, premature projection capping, reduced-motion replay, missing available identity on record signals, and hidden-page tooltip cleanup. Each was repaired with a focused regression. Follow-up review tightened crash-boundary capture and consumer-identity guards; regressions cover an actual read → crash → restart sequence and conflicting actor metadata. Final independent source re-review confirms **P0 = 0; P1 = 0; P2 = 0** within this repair scope. The reviewer performed read-only source inspection; root ran the gates above.

## Remaining acceptance

Follow [manual section 16](../../docs/MANUAL_TEST_CHECKLIST.md#16-focused-chart-tabs-and-tooltips--current-slice) and [CHART_LIVE_UI_FIXES.md](../../docs/CHART_LIVE_UI_FIXES.md). User acceptance, natural background-tab behavior, physical touch-device interaction, and production-server browser execution remain unclaimed. Browser evidence is from the development server; the production build itself passes. Real Kafka broker behavior is outside this browser simulator's boundary.
