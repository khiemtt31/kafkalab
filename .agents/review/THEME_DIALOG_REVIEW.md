# Theme and dialog removal review — 2026-10-05

Status: `READY_FOR_MANUAL_TEST`. Implementation and permitted static checks are complete. Browser checks and user acceptance remain open in [manual section 15](../../docs/MANUAL_TEST_CHECKLIST.md#15-remove-inspect-step-and-choose-light-or-dark--current-slice).

## Implemented

- Deleted `StepInspector.tsx`, both route callers, overlay styles/animation, and all canvas inspection callbacks. Native React Flow selection, graph controls, positions, viewport, and packet behavior remain.
- Explicit Customize explanations open normal Activity, with a focus handoff to its heading. The existing embedded `InspectorPanel` retains actual record state, producer/time, latest application attempt, failure reason, and eligible retry. Retry moves focus to the retained detail heading before its action disappears. The 18-record Activity window uses the existing failure-pinning projection.
- Added the far-right shared-header moon/sun button and light/dark app, graph, minimap, form, metric/chart/tooltip, Guide, and Practice styling. The header uses two rows below 800px.
- Theme defaults to light and uses independent validated `kafkalab:theme:v1` JSON `{ "version": 1, "theme": "light" | "dark" }`. A synchronous head script applies the saved choice before paint; `useSyncExternalStore` keeps the server snapshot consistent. Read/write failures retain session usability, and storage events synchronize the preference across tabs. Theme does not call the simulator or progress stores.
- Adapted one existing obsolete route-workspaces test case to normal Activity/focus behavior. No tests were added or run.

## Permitted verification

| Check | Final result |
| --- | --- |
| `npm run typecheck` | PASS, exit 0 |
| `npm run lint` | PASS, exit 0, no warnings |
| `npm run build` | PASS, exit 0; latest source compiled and all static routes generated |
| Fresh independent source review | PASS; `/root/fresh_learning_review`, 0 P0, 0 P1, 0 actionable P2 |
| Full automatic test gate / Vitest | NOT RUN; current instructions prohibit tests without an explicit request |
| Browser UI, reload flash, keyboard/mobile/storage interaction | NOT RUN; manual section 15 remains unchecked |

An intermediate typecheck overlapped Next build regeneration and encountered missing `.next/types` files. After the build finished, typecheck was rerun successfully. Keep builds and standalone typechecks sequential. An intermediate lint error from a ref-capturing shared actions object was resolved by keeping the original store actions separate from detail-navigation callbacks. The final build ran after the successful final typecheck/lint.

The fresh review identified four P2 issues during implementation: worker-navigation focus, light idle icon contrast, dark small red chart text, and retry-action focus. Root repaired them and the reviewer reread the current source before reporting zero remaining findings. This is source evidence, not a claim that browser interactions passed.

## Local handoff

Root stopped the prior server, verified ports were free, built, and started one production server with `npm run start`. It reported ready on `http://localhost:3000`. `lsof -nP -iTCP:3000-3017 -sTCP:LISTEN` showed only PID `34809` on port 3000 and no listeners on 3001–3017 at handoff. PID/session details are transient; inspect the actual listener before stopping or changing server mode. No current HTTP route smoke or browser run was performed.

No dependencies, Kafka rules, archives, commits, pushes, or deployments were added. Historical 167-test/browser evidence does not certify this slice. User acceptance is required before `COMPLETE`.
