# Flow and Guide learning redesign review — 2026-10-09

Status: **SOURCE REVIEW APPROVED**. Final independent source re-review has **P0 = 0; P1 = 0; actionable P2 = 0**. Root's final engineering gates and the user's browser/manual acceptance are separate requirements. This review does not declare the slice `COMPLETE`.

## Scope and evidence

Reviewed the current [learning contract](../../docs/FLOW_GUIDE_LEARNING_REDESIGN.md), current acceptance/manual requirements, Flow shell and diagnosis, all seven chart projections, canvas/node/details rendering, Guide lessons and captured evidence, experiment runner/commit guard, learning persistence, styles, and relevant tests. The current contract supersedes historical toolbar-only and hover-first teaching rules. Source review preserved Practice, removed Customize/INSPECT STEP, simulator independence, and theme boundaries.

Read-next versus committed-next positions, partition ownership, and restart behavior were checked against [Apache Kafka 4.3 KafkaConsumer documentation](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html). Graph processing/outcome nodes are derived teaching steps; commits remain group checkpoints rather than proof of application success or removal of records. Local failure blocking is a simulator safety policy.

After all reported fixes landed, the reviewer independently ran `npx vitest run tests/components/guide-experiments.test.tsx tests/components/chart-tooltips.test.tsx tests/components/flow-learning.test.ts`: **3 files / 27 tests passed**. This exercises the repaired additional-read commit rejection, changed assignment scope, keyed exact identities, details focus cleanup, retry/commit separation, stale workflow evidence, and diagnosis. `git diff --check` also passed. Root owns final typecheck, lint, full relevant tests, and the sequential production build; final outcomes belong in `CHECKLIST.md`.

No browser session was available for this review. Source and jsdom assertions do not prove actual rendering, touch behavior, natural backgrounding, first paint, 320px/390px overflow, zoom readability, or the learner's acceptance. Manual section 17 remains required.

## Findings already repaired and re-reviewed

- P2: Optional injected adapters previously displayed global scenario actions targeting a different simulator. Flow now renders the workbench only for the app runtime's action object; alternate adapters retain their own chart/controls. A regression covers isolation and a reactive Flow retry/commit cycle.
- P2: Failed-read copy previously attributed an observed attempt to the current owner after rebalance. The diagnosis now names the delivery's actual consumer separately from current ownership, with a rebalance regression.
- P2: Activity formerly labelled source-wide simulator events as selected-group events. Flow and canvas labels now identify simulator-wide Activity while Delivery remains group-scoped.
- P2: Missing producer provenance previously chose the first known producer. A labelled unknown-origin teaching node now replaces that guess, with no invented record-transit source and a regression for older records.
- P2: The visible diagnosis formerly omitted available stored identity/attempt. It now includes actual ID, partition, offset, key, and attempt while deriving outcome and checkpoint explanations from live state.
- P2: Optional details could accumulate after keyboard focus left a node. A focus-containment listener now dismisses details when focus moves outside the owning node/wrapper and its portal, retaining F2, Escape return, outside-pointer, and hidden-page cleanup.
- P2: Focused layouts deliberately use canonical temporary geometry rather than overwriting whole-system saves. Current contract/manual wording now distinguishes that behavior from persistent independent whole-system layouts.
- P1: Captured-run commit eligibility previously allowed later reads while the original attempt/checkpoint stayed unchanged. The final guard now verifies captured record identity/attempt, group, actual owner, exact owner assignment scope, and both owner/snapshot read and saved positions for every lane that `commitConsumer` affects. Real additional-reading/pause and new-owned-lane regressions pass; stale commits return unchanged evidence without mutating the simulator.
- P2: Keyed lesson IDs were omitted from its rendered paths/table. The exact disclosure now exposes all bounded captured stored IDs, alongside visible key/partition/offset/value evidence, with a passing regression.
- P2: Current Project Spec scroll-hint wording differed from the optional exact-table implementation. Current requirements now describe essential visible paths plus labelled keyboard-focusable optional exact comparisons with their documented 420px local width. Historical 540px browser evidence remains explicitly historical.

## Remaining actionable findings

None within this review scope. All reported P1/P2 findings were repaired and independently re-reviewed against the final sources.

## Source assessment

The seven projections continue to derive from one snapshot with stable entity/teaching-summary IDs, no graph node per message, and edges filtered to visible endpoints. Focus respects actual partition ownership and replication assignments; worker scaling can show all owners and idle members. Retry, success-before-save replay, and premature-commit skipping remain distinct. Unknown source/broker state is labelled instead of invented.

Lesson and prediction selection stay passive. Only explicit Run replaces the simulator with a paused bounded fixture; results are captured separately and labelled as captured evidence. Workflow/group replacement invalidates captured focus and manual commit; restored learning progress does not rerun an experiment. Existing Send example / Read record / Save progress remains distinct and manually committed. Learning reset, Practice reset, simulator reset, and theme retain independent state.

Native tabs/keyboard navigation, labelled predictions and progress, result announcements, reference disclosures, labelled focusable exact-table scrolling, and text beyond color are present in source. Essential outcome copy is visible without every tooltip. Styles stack controls/evidence on narrow screens and use shared light/dark tokens; reduced motion retains static semantic state. Actual layout and visual learning impact remain manual acceptance items.

Post-approval CSS re-review: `flow-learning.css` now targets `.kl-workspace.kl-learning-workspace` for page sizing. Its two-class specificity keeps `height: auto`, visible overflow, and the mobile minimum height authoritative over the one-class `.kl-workspace` sizing in shared workspace/theme styles regardless of stylesheet order. The challenge can extend the page naturally instead of painting over the footer. Sidebar/canvas stacking below 1200px remains bounded; the standalone adapter rule keeps its own single column. `git diff --check` passes. No JavaScript changed in this hardening, no additional tests were needed, and source approval/P0/P1/P2 counts remain unchanged. Actual rendered mobile acceptance remains pending.
